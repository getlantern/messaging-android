package io.lantern.messaging

import java.util.concurrent.atomic.AtomicReference

class ValueMonitor<V>(initialValue: V) : java.lang.Object() {
    private val value = AtomicReference(initialValue)

    fun set(v: V) {
        value.set(v)
        synchronized(this) {
            notifyAll()
        }
    }

    @Synchronized
    fun get(timeout: Long): V {
        try {
            wait(timeout)
        } catch (t: Throwable) {
        }
        return value.get()
    }
}
