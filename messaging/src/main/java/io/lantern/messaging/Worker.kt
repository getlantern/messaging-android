package io.lantern.messaging

import java.io.Closeable
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import mu.KotlinLogging

/**
 * Worker is a base type for classes that coordinate work using a single-threaded work queue. It
 * provides facilities for submitting work to the queue for immediate or delayed execution.
 */
internal abstract class Worker(
    protected val messaging: Messaging,
    name: String,
) : Closeable {
    protected val logger = KotlinLogging.logger("${messaging.logger.name}-$name")

    internal val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${messaging.name}-$name-executor")
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

    internal fun submitDelayed(delayMillis: Long, cmd: () -> Unit) {
        try {
            executor.schedule(
                {
                    try {
                        cmd()
                    } catch (t: Throwable) {
                        logger.error(t.message, t)
                    }
                },
                delayMillis, TimeUnit.MILLISECONDS
            )
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

    override fun close() {
        executor.shutdownNow()
    }
}
