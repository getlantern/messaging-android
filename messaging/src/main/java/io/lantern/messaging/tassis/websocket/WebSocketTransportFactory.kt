package io.lantern.messaging.tassis.websocket

import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import io.lantern.messaging.tassis.TransportFactory
import mu.KotlinLogging
import okhttp3.*
import okio.ByteString
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

private val logger = KotlinLogging.logger {}

class AbnormalCloseException(message: String) : RuntimeException(message)

open class WebSocketTransportFactory(
    private val url: String,
    connectTimeoutMillis: Long = 15000,
    pingIntervalMillis: Long = connectTimeoutMillis,
) : TransportFactory {
    val client =
        OkHttpClient().newBuilder().connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
            .writeTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
            .readTimeout(pingIntervalMillis * 2, TimeUnit.MILLISECONDS)
            .callTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
            .pingInterval(pingIntervalMillis, TimeUnit.MILLISECONDS).retryOnConnectionFailure(false)
            .build()

    override fun connect(handler: MessageHandler) {
        try {
            val request = Request.Builder().url(getUrl()).build()
            val closedByHandler = AtomicBoolean()
            client.newWebSocket(request, buildListener(handler, closedByHandler))
        } catch (t: Throwable) {
            handler.onConnectError(t)
        }
    }

    protected open fun getUrl(): String = url

    protected open fun buildListener(
        handler: MessageHandler,
        closedByHandler: AtomicBoolean
    ): WebSocketListener = WSListener(this, handler, closedByHandler)

    internal open fun buildTransport(
        webSocket: WebSocket,
        closedByHandler: AtomicBoolean
    ): Transport = object : Transport {
        override fun send(data: ByteArray) {
            webSocket.send(ByteString.of(*data))
        }

        override fun forceClose() {
            webSocket.close(1001, null)
        }

        override fun close() {
            closedByHandler.set(true)
            webSocket.close(1000, null)
        }
    }
}

open class WSListener(
    protected val factory: WebSocketTransportFactory,
    protected val handler: MessageHandler,
    protected val closedByHandler: AtomicBoolean,
) : WebSocketListener() {
    override fun onOpen(webSocket: WebSocket, response: Response) {
        handler.setTransport(factory.buildTransport(webSocket, closedByHandler))
    }

    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        handler.onConnectError(t)
    }

    override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        handler.onMessage(bytes.toByteArray())
    }

    override fun onClosed(webSocket: WebSocket, code: Int, reason: String) {
        if (closedByHandler.get()) {
            // the handler already knows we closed, don't bother reporting
            return
        }
        doOnClose(code, reason)
    }

    protected fun doOnClose(code: Int, reason: String?) {
        if (code != 1000) {
            handler.onClose(AbnormalCloseException("websocket closed abnormally: $reason"))
        } else {
            handler.onClose(null)
        }
    }
}