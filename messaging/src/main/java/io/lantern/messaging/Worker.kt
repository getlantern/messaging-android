package io.lantern.messaging

import java.io.Closeable
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import mu.KotlinLogging

/**
 * Worker is a base type for classes that coordinate work using a single-threaded work queue. It
 * provides facilities for submitting work to the queue for immediate or delayed execution.
 */
internal abstract class Worker(
    protected val messaging: Messaging,
    name: String,
    private val retryDelayMillis: Long
) : Closeable {
    protected val logger = KotlinLogging.logger("${messaging.logger.name}-$name")

    internal val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${messaging.name}-$name-executor")
    }

    private val retries = LinkedBlockingQueue<() -> Unit>()

    init {
        logger.debug("will automatically retry every ${retryDelayMillis}ms")
        executor.scheduleAtFixedRate(
            {
                while (true) {
                    retries.poll()?.let { submit(it) } ?: return@scheduleAtFixedRate
                }
            },
            retryDelayMillis, retryDelayMillis, TimeUnit.MILLISECONDS
        )
    }

    internal fun submit(cmd: () -> Unit) {
        try {
            executor.submit {
                try {
                    cmd()
                } catch (t: Throwable) {
                    logger.error(t.message, t)
                }
            }
        } catch (t: Throwable) {
            logger.error(t.message, t)
        }
    }

    internal fun <T> submitForValue(cmd: () -> T): T {
        try {
            return executor.submit(Callable { cmd() }).get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }
    }

    internal fun retryFailed(cmd: () -> Unit) {
        retries.add(cmd)
    }

    override fun close() {
        executor.shutdownNow()
    }
}
