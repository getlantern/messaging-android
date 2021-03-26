package io.lantern.messaging.tassis.websocket

import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import io.lantern.messaging.tassis.TransportFactory
import mu.KotlinLogging
import org.java_websocket.client.WebSocketClient
import org.java_websocket.drafts.Draft_6455
import org.java_websocket.framing.CloseFrame
import org.java_websocket.handshake.ServerHandshake
import java.net.URI
import java.nio.ByteBuffer

private val logger = KotlinLogging.logger {}

open class WebSocketTransportFactory(
    private val url: String,
    private val connectTimeoutMillis: Int = 15000,
    private val connectionLostTimeoutSeconds: Int = 60
) : TransportFactory {
    override fun connect(handler: MessageHandler) {
        try {
            val transport =
                buildTransport(url, handler, connectTimeoutMillis, connectionLostTimeoutSeconds)
            handler.setTransport(transport)
            transport.connect()
        } catch (t: Throwable) {
            handler.onConnectError(t)
        }
    }

    protected open fun buildTransport(
        url: String, handler: MessageHandler, connectTimeoutMillis: Int,
        connectionLostTimeoutSeconds: Int
    ): WebSocketTransport {
        return WebSocketTransport(url, handler, connectTimeoutMillis, connectionLostTimeoutSeconds)
    }
}

open class WebSocketTransport(
    url: String,
    private val handler: MessageHandler,
    connectTimeoutMillis: Int,
    connectionLostTimeoutSeconds: Int
) :
    WebSocketClient(URI(url), Draft_6455(), null, connectTimeoutMillis), Transport {

    private var closedByHandler = false

    init {
        // enable checking for lost connections, which helps keep the connection alive and also
        // closes it when it seems to have been disconnected
        connectionLostTimeout = connectionLostTimeoutSeconds
    }

    override fun onOpen(handshakedata: ServerHandshake?) {
        // we ignore this. In practice, once connected, the server will send an authentication
        // challenge. Once the client sees this and responds to it as appropriate, it will notify
        // its delegate that we're connected
    }

    override fun onMessage(message: String?) {
        logger.debug("ignoring text message")
    }

    override fun onMessage(bytes: ByteBuffer?) {
        // Note - from what I can tell, this will always be the complete message, even if it was
        // fragmented into multiple frames
        handler.onMessage(bytes)
    }

    override fun close() {
        closedByHandler = true
        super.close()
    }

    /**
     * This is really just for testing
     */
    fun forceClose() {
        super.closeBlocking()
    }

    override fun onClose(code: Int, reason: String?, remote: Boolean) {
        if (closedByHandler) {
            // the handler already knows we closed, don't bother reporting
            return
        }
        doOnClose(code, reason, remote)
    }

    protected fun doOnClose(code: Int, reason: String?, remote: Boolean) {
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