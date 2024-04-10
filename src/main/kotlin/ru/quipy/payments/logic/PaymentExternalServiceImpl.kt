package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.tools.FAAQueue
import ru.quipy.payments.logic.tools.Queue
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.math.min


// Advice: always treat time as a Duration
@Service
class PaymentExternalServiceImpl(
    private val properties: List<ExternalServiceProperties>,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val accountProcessingWorkers = properties.map {
        it.toAccountProcessingWorker()
    }


    private val httpClientExecutor = Executors.newFixedThreadPool(1000)

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    private fun pickAccountProcessingWorker(
        paymentId: UUID, amount: Int, paymentStartedAt: Long
    ) {
        val minimumProcessingTime = properties.minOf { p -> p.request95thPercentileProcessingTime }
        while (true) {
            if ((Duration
                    .ofMillis(paymentOperationTimeout.toMillis() - (now() - paymentStartedAt))
                        - minimumProcessingTime).isNegative
            ) {
                val transactionId = UUID.randomUUID()

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
            }
            for (accountProcessingWorker in accountProcessingWorkers) {
                if ((Duration
                        .ofMillis(paymentOperationTimeout.toMillis() - (now() - paymentStartedAt))
                            - accountProcessingWorker.info.request95thPercentileProcessingTime)
                        .toMillis()
                    * accountProcessingWorker.info.speedPerMillisecond
                    > accountProcessingWorker.paymentQueue.length()

                ) {
                    logger.warn("[${accountProcessingWorker.info.accountName}] Payment $paymentId has chosen account. Already passed: ${now() - paymentStartedAt} ms")
                    accountProcessingWorker.enqueuePayment(paymentId, amount, paymentStartedAt)
                    return
                } else {
                    continue
                }
            }
            logger.warn(
                "Payment $paymentId couldn't choose account. Information about queue length of accounts: [${
                    accountProcessingWorkers.joinToString { apw ->
                        "${apw.info.accountName} - ${apw.paymentQueue.length()}"
                    }
                }]. Already passed: ${now() - paymentStartedAt} ms"
            )
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("Payment $paymentId started choosing account. Already passed: ${now() - paymentStartedAt} ms")
        pickAccountProcessingWorker(paymentId, amount, paymentStartedAt)
    }

    fun ExternalServiceProperties.toAccountProcessingWorker(): AccountProcessingWorker =
        AccountProcessingWorker(AccountProcessingInfo(this))

    inner class AccountProcessingWorker(
        val info: AccountProcessingInfo
    ) {
        @OptIn(ExperimentalCoroutinesApi::class)
        private val requestScope = CoroutineScope(Dispatchers.IO.limitedParallelism(100))

        @OptIn(ExperimentalCoroutinesApi::class)
        private val queueProcessingScope = CoroutineScope(Dispatchers.IO.limitedParallelism(100))

        private val requestCounter = NonBlockingOngoingWindow(info.maxParallelRequests)
        private val rateLimiter = RateLimiter(info.rateLimitPerSec)
        val paymentQueue: Queue<PaymentInfo> = FAAQueue()

        private fun sendRequest(
            transactionId: UUID, paymentId: UUID, amount: Int, paymentStartedAt: Long
        ) = requestScope.launch {
            if (Duration.ofMillis(now() - paymentStartedAt) + info.request95thPercentileProcessingTime > paymentOperationTimeout) {
                requestCounter.releaseWindow()
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return@launch
            }

            logger.error("[${info.accountName}] Payment started sending request for txId: $transactionId, payment: $paymentId. Already passed: ${now() - paymentStartedAt} ms")
            val request = Request.Builder().run {
                url("http://localhost:1234/external/process?serviceName=${info.serviceName}&accountName=${info.accountName}&transactionId=$transactionId")
                post(emptyBody)
            }.build()

            try {
                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[${info.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[${info.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error(
                            "[${info.accountName}] Payment failed for txId: $transactionId, payment: $paymentId", e
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                requestCounter.releaseWindow()
            }
        }

        fun enqueuePayment(
            paymentId: UUID, amount: Int, paymentStartedAt: Long
        ) = queueProcessingScope.launch {
            paymentQueue.enqueue(PaymentInfo(paymentId, amount, paymentStartedAt))
            logger.warn("[${info.accountName}] Added payment $paymentId in queue. Current number ${paymentQueue.length()}. Already passed: ${now() - paymentStartedAt} ms")
        }

        private val processQueue = queueProcessingScope.launch {
            while (true) {
                if (paymentQueue.length() != 0L) {
                    val windowResult = requestCounter.putIntoWindow()
                    if (windowResult is NonBlockingOngoingWindow.WindowResponse.Success) {
                        while (!rateLimiter.tick()) {
                            continue
                        }
                    } else {
                        continue
                    }
                } else {
                    continue
                }

                val payment = paymentQueue.dequeue()
                if (payment != null) {
                    logger.warn("[${info.accountName}] Submitting payment request for payment ${payment.id}. Already passed: ${now() - payment.startedAt} ms")
                    val transactionId = UUID.randomUUID()
                    logger.info("[${info.accountName}] Submit for ${payment.id} , txId: $transactionId")
                    paymentESService.update(payment.id) {
                        it.logSubmission(
                            success = true, transactionId, now(), Duration.ofMillis(now() - payment.startedAt)
                        )
                    }

                    sendRequest(transactionId, payment.id, payment.amount, payment.startedAt)
                }
            }
        }
    }

    class AccountProcessingInfo(
        properties: ExternalServiceProperties
    ) {
        val serviceName = properties.serviceName
        val accountName = properties.accountName
        val maxParallelRequests = properties.parallelRequests
        val rateLimitPerSec = properties.rateLimitPerSec
        val request95thPercentileProcessingTime = properties.request95thPercentileProcessingTime
        val speedPerMillisecond = min(
            maxParallelRequests.toDouble() / (request95thPercentileProcessingTime.toMillis()),
            rateLimitPerSec.toDouble() / 1000
        )
    }

    data class PaymentInfo(
        val id: UUID, val amount: Int, val startedAt: Long
    )
}

public fun now() = System.currentTimeMillis()