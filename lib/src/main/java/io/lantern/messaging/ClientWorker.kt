package io.lantern.messaging

import io.lantern.messaging.tassis.Client
import io.lantern.messaging.tassis.ClientDelegate
import io.lantern.messaging.tassis.TransportFactory
import java.util.concurrent.Semaphore
import kotlin.math.pow

internal abstract class ClientWorker<D : ClientDelegate, C : Client<D>>(
    private val transportFactory: TransportFactory,
    messaging: Messaging,
    name: String,
    private val redialBackoffMillis: Long,
    private val maxRedialDelayMillis: Long,
    private val autoConnect: Boolean = false
) : Worker(messaging, "$name-client"), ClientDelegate {
    private val lock = Semaphore(1)
    private var client: C? = null
    private var cbAfterConnect: ((C) -> Unit)? = null
    private var connsecutiveFailures = -1

    init {
        autoConnectIfNecessary()
    }

    internal fun withClient(cb: (C) -> Unit) {
        submit {
            client?.let { cb(it) } ?: connectThen(cb)
        }
    }

    private fun connectThen(cb: (C) -> Unit) {
        if (connsecutiveFailures > -1) {
            val redialDelay =
                (redialBackoffMillis * 2.0.pow(connsecutiveFailures)).toLong()
            val actualRedialDelay =
                if (maxRedialDelayMillis < redialDelay) maxRedialDelayMillis else redialDelay
            logger.debug("due to $connsecutiveFailures previous errors connecting to tassis, will wait ${actualRedialDelay}ms before dialing again")
            Thread.sleep(actualRedialDelay)
        }
        cbAfterConnect = cb
        val newClient = buildClient()
        transportFactory.connect(newClient)
    }

    abstract fun buildClient(): C

    fun onConnected(client: C) {
        submit {
            lock.release()
            this.client = client
            connsecutiveFailures = -1
            cbAfterConnect?.let { it(client) }
            cbAfterConnect = null
        }
    }

    override fun onConnectError(err: Throwable) {
        logger.error("error connecting client: ${err.message}")
        submit {
            lock.release()
            cbAfterConnect?.let { submit { withClient(it) } }
            cbAfterConnect = null
        }
    }

    override fun onClose(err: Throwable?) {
        submit {
            if (err != null) {
                logger.error("anonymous tassis client closed with error ${err.message}")
                if (cbAfterConnect != null) {
                    connsecutiveFailures++
                    lock.release()
                    cbAfterConnect?.let { submit { withClient(it) } }
                    cbAfterConnect = null
                }
            } else {
                logger.debug("anonymous tassis client closed normally")
                submit {
                    client = null
                }
            }
            autoConnectIfNecessary()
        }
    }

    override fun close() {
        submit {
            client?.let { it.close() }
            client = null
            super.close()
        }
    }

    private fun autoConnectIfNecessary() {
        if (!autoConnect) {
            return
        }
        withClient { logger.debug("auto connected") }
    }
}