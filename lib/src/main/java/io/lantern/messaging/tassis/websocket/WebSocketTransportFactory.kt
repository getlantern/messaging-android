package io.lantern.messaging.tassis.websocket

import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import io.lantern.messaging.tassis.TransportFactory
import mu.KotlinLogging
import org.java_websocket.client.WebSocketClient
import org.java_websocket.framing.CloseFrame
import org.java_websocket.handshake.ServerHandshake
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

private val logger = KotlinLogging.logger {}

class WebSocketTransportFactory(
    private val url: String,
    private val connectTimeoutMillis: Long = 15000,
    private val connectionLostTimeoutSeconds: Int = 30
) : TransportFactory {
    override fun build(handler: MessageHandler, cb: Callback<Transport>) {
        try {
            val transport = WebSocketTransport(url, handler, connectionLostTimeoutSeconds)
            transport.connectBlocking(
                connectTimeoutMillis,
                TimeUnit.MILLISECONDS
            ) // todo: use timeout
            cb.onSuccess(transport)
        } catch (t: Throwable) {
            cb.onError(t)
        }
    }
}

class WebSocketTransport(
    url: String,
    private val handler: MessageHandler,
    connectionLostTimeoutSeconds: Int = 30
) :
    WebSocketClient(URI(url)), Transport {
    init {
        // enable checking for lost connections, which helps keep the connection alive and also
        // closes it when it seems to have been disconnected
        connectionLostTimeout = connectionLostTimeoutSeconds
    }

    override fun onOpen(handshakedata: ServerHandshake?) {
        // ignore
    }

    override fun onMessage(message: String?) {
        logger.debug("ignoring text message")
    }

    override fun onMessage(bytes: ByteBuffer?) {
        handler.onMessage(bytes)
    }

    override fun onClose(code: Int, reason: String?, remote: Boolean) {
        if (code != CloseFrame.NORMAL) {
            handler.onClose(AbnormalCloseException("websocket closed abnormally: $reason"))
        } else {
            handler.onClose(null)
        }
    }

    override fun onError(ex: Exception?) {
        handler.onClose(ex)
    }
}

class AbnormalCloseException(message: String) : RuntimeException(message)