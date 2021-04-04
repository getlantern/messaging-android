package io.lantern.messaging

import mu.KotlinLogging
import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

internal abstract class Worker(
    protected val messaging: Messaging,
    name: String,
    retryDelayMillis: Long? = null
) : Closeable {
    protected val logger = KotlinLogging.logger("${messaging.logger.name}-${name}")

    internal val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${messaging.name}-${name}-executor")
    }

    private val retries = LinkedBlockingQueue<() -> Unit>()

    init {
        if (retryDelayMillis != null) {
            logger.debug("will automatically retry every ${retryDelayMillis}ms")
            executor.scheduleAtFixedRate({
                while (true) {
                    retries.poll()?.let { submit(it) } ?: return@scheduleAtFixedRate
                }
            }, retryDelayMillis, retryDelayMillis, TimeUnit.MILLISECONDS)
        }
    }

    internal fun submit(cmd: () -> Unit) {
        try {
            executor.submit {
                try {
                    cmd()
                } catch (t: Throwable) {
                    logger.error(t.message, t)
                    retryFailed(cmd)
                }
            }
        } catch (t: Throwable) {
            logger.error(t.message)
        }
    }

    protected fun retryFailed(cmd: () -> Unit) {
        retries.add(cmd)
    }

    override fun close() {
        executor.shutdownNow()
    }
}