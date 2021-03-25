package io.lantern.messaging

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal abstract class Worker(
    protected val messaging: Messaging,
    private val retryDelayMillis: Long,
    name: String
) {
    internal val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${messaging.name}-${name}-executor")
    }

    internal fun submit(cmd: () -> Unit) {
        executor.submit {
            try {
                cmd()
            } catch (t: Throwable) {
                messaging.logger.error(t.message, t)
                retryFailed(cmd)
            }
        }
    }

    protected fun schedule(delayMillis: Long, cmd: () -> Unit) {
        executor.schedule({
            try {
                cmd()
            } catch (t: Throwable) {
                messaging.logger.error(t.message, t)
            }
        }, delayMillis, TimeUnit.MILLISECONDS)
    }

    protected fun retryFailed(cmd: () -> Unit) {
        schedule(retryDelayMillis, cmd)
    }
}