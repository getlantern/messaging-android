package io.lantern.messaging

import io.lantern.messaging.tassis.Client
import io.lantern.messaging.tassis.ClientDelegate
import io.lantern.messaging.tassis.TransportFactory
import kotlin.math.pow

/**
 * This handles client connections in a single threaded executor. All connecting and handling of
 * disconnects happens on this single thread in order to ensure that we only ever have one active
 * client and that whenever we encounter an error while connecting or a client is closed before we
 * could submit an operation, we automatically retry the pending operation once successfully
 * connected.
 */
internal abstract class ClientWorker<D : ClientDelegate, C : Client<D>>(
    private val transportFactory: TransportFactory,
    messaging: Messaging,
    name: String,
    private val redialBackoffMillis: Long,
    private val maxRedialDelayMillis: Long,
    private val autoConnect: Boolean = false
) : Worker(messaging, "$name-client"), ClientDelegate {
    private var client: C? = null
    private var currentlyConnecting = false
    private val cbsAfterConnect = ArrayList<((C) -> Unit)>()
    private var consecutiveFailures = -1

    init {
        autoConnectIfNecessary()
    }

    internal fun withClient(cb: (C) -> Unit) {
        submit {
            client?.let {
                cb(it)
            } ?: connectThen(cb)
        }
    }

    private fun connectThen(cb: (C) -> Unit) {
        cbsAfterConnect.add(cb)

        if (currentlyConnecting) {
            return
        }

        if (consecutiveFailures > -1) {
            val redialDelay =
                (redialBackoffMillis * 2.0.pow(consecutiveFailures)).toLong()
            val actualRedialDelay =
                if (maxRedialDelayMillis < redialDelay) maxRedialDelayMillis else redialDelay
            logger.debug("due to $consecutiveFailures previous errors communicating with tassis, will wait ${actualRedialDelay}ms before dialing again")
            Thread.sleep(actualRedialDelay)
        }

        logger.debug("connecting")
        currentlyConnecting = true
        val newClient = buildClient()
        transportFactory.connect(newClient)
    }

    abstract fun buildClient(): C

    fun onConnected(client: C) {
        submit {
            logger.debug("successfully connected")
            this.client = client
            consecutiveFailures = -1
            currentlyConnecting = false
            cbsAfterConnect.forEach { it(client) }
            cbsAfterConnect.clear()
        }
    }

    override fun onConnectError(err: Throwable) {
        submit {
            logger.error("error connecting client: ${err.message}")
            consecutiveFailures++
            currentlyConnecting = false
            // note - we leave the cbsAfterConnect in place so that they have a chance to be run
            withClient {
                // this is simply invoked to force an attempt at reconnecting
            }
        }
    }

    override fun onClose(err: Throwable?) {
        submit {
            if (err != null) {
                logger.error("closed with error ${err.message}")
            } else {
                logger.debug("closed normally")
            }
            // only clear client if we're not currently still in the process of connecting
            // otherwise, we expect that onConnectError will be called, at which point
            // we'll clear the connection
            client = null
            autoConnectIfNecessary()
        }
    }

    override fun close() {
        submit {
            client?.let {
                it.close()
                client = null
            }
            super.close()
        }
    }

    private fun autoConnectIfNecessary() {
        if (!autoConnect) {
            return
        }
        withClient { logger.trace("auto connected") }
    }
}