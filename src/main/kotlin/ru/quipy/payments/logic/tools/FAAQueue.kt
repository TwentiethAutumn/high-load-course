package ru.quipy.payments.logic.tools

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicReferenceArray

class FAAQueue<E>: Queue<E> {
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)
    private val head: AtomicReference<Segment>
    private val tail: AtomicReference<Segment>

    init {
        val dummy = Segment(0)
        head = AtomicReference(dummy)
        tail = AtomicReference(dummy)
    }

    private fun shouldTryToDequeue(): Boolean {
        while (true) {
            val curDequeueIndex = deqIdx.get()
            val curEnqueueIndex = enqIdx.get()
            if (curEnqueueIndex == enqIdx.get()) {
                return curDequeueIndex < curEnqueueIndex
            }
        }
    }

    private fun findSegment(start: Segment, id: Long): Segment {
        var curSegment = start
        while (curSegment.id < id) {
            if (curSegment.next.compareAndSet(null, Segment(id))) {
                return curSegment.next.get()!!
            }
            curSegment = curSegment.next.get()!!
        }
        return curSegment
    }

    private fun moveTailForward(newTail: Segment) {
        val curTail = tail.get()
        if (curTail.id >= newTail.id) {
            return
        }
        if (curTail.next.compareAndSet(null, newTail)) {
            tail.compareAndSet(curTail, newTail)
        }
    }

    private fun moveHeadForward(newHead: Segment) {
        val curHead = head.get()
        if (curHead.id >= newHead.id) {
            return
        }
        curHead.next.compareAndSet(null, newHead)
        head.compareAndSet(curHead, newHead)
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.get()
            val i = enqIdx.getAndIncrement()
            val id = i / SEGMENT_SIZE
            val num = i % SEGMENT_SIZE
            val s = findSegment(curTail, id)
            moveTailForward(s)
            if (s.cells.compareAndSet(num.toInt(), null, element)) {
                return
            }
        }
    }

    override fun dequeue(): E? {
        while (true) {
            if (!shouldTryToDequeue()) return null
            val curHead = head.get()
            val i = deqIdx.getAndIncrement()
            val id = i / SEGMENT_SIZE
            val num = i % SEGMENT_SIZE
            val s = findSegment(curHead, id)
            moveHeadForward(s)
            if (!s.cells.compareAndSet(num.toInt(), null, POISONED)) {
                val element = s.cells.get(num.toInt()) as E
                s.cells.compareAndSet(num.toInt(), element, null)
                return element
            }
        }
    }

    override fun length(): Long {
        val dequeIndex = deqIdx.get()
        val enqueueIndex = enqIdx.get()
        return enqueueIndex - dequeIndex
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 10

// TODO: poison cells with this value.
private val POISONED = Any()