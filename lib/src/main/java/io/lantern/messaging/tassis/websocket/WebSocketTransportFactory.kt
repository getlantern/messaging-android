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

private val logger = KotlinLogging.logger {}

class WebSocketTransportFactory(private val url: String) : TransportFactory {
    override fun build(handler: MessageHandler, cb: Callback<Transport>) {
        try {
            val transport = WebSocketTransport(url, handler)
            transport.connectBlocking() // todo: use timeout
            cb.onSuccess(transport)
        } catch (t: Throwable) {
            cb.onError(t)
        }
    }
}

class WebSocketTransport(url: String, private val handler: MessageHandler) :
    WebSocketClient(URI(url)), Transport {
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
            handler.onClose(AbnormalCloseException("websocket closed abnormally: ${reason}"));
        } else {
            handler.onClose(null)
        }
    }

    override fun onError(ex: Exception?) {
        handler.onClose(ex)
    }
}

class AbnormalCloseException(message: String) : RuntimeException(message)