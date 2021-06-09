package io.lantern.messaging.tassis.websocket

import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import io.lantern.messaging.tassis.TransportFactory
import java.util.concurrent.TimeUnit
import mu.KotlinLogging
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString

private val logger = KotlinLogging.logger {}

open class WebSocketTransportFactory(
    private val url: String,
    connectTimeoutMillis: Long = 10000,
    writeTimeoutMillis: Long = connectTimeoutMillis,
    callTimeoutMillis: Long = writeTimeoutMillis,
    pingIntervalMillis: Long = connectTimeoutMillis,
    readTimeoutMillis: Long = pingIntervalMillis * 2,
) : TransportFactory {
    val client =
        OkHttpClient().newBuilder().connectTimeout(connectTimeoutMillis, TimeUnit.MILLISECONDS)
            .writeTimeout(writeTimeoutMillis, TimeUnit.MILLISECONDS)
            .callTimeout(callTimeoutMillis, TimeUnit.MILLISECONDS)
            .pingInterval(pingIntervalMillis, TimeUnit.MILLISECONDS)
            .readTimeout(readTimeoutMillis, TimeUnit.MILLISECONDS)
            .retryOnConnectionFailure(false) // don't retry on connect failures, we handle those failures at a higher level
            .build()

    override fun connect(handler: MessageHandler) {
        try {
            val request = Request.Builder().url(getUrl()).build()
            client.newWebSocket(request, buildListener(handler))
        } catch (t: Throwable) {
            handler.onFailure(t)
        }
    }

    protected open fun getUrl(): String = url

    protected open fun buildListener(
        handler: MessageHandler
    ): WebSocketListener = WSListener(this, handler)

    internal open fun buildTransport(
        webSocket: WebSocket
    ): Transport = WSTransport(webSocket)
}

internal open class WSTransport(
    private val webSocket: WebSocket
) : Transport {
    override fun send(data: ByteArray) {
        webSocket.send(ByteString.of(*data))
    }

    override fun cancel() {
        webSocket.cancel()
    }

    override fun close() {
        webSocket.close(1000, null)
    }
}

open class WSListener(
    private val factory: WebSocketTransportFactory,
    private val handler: MessageHandler,
) : WebSocketListener() {
    override fun onOpen(webSocket: WebSocket, response: Response) {
        handler.setTransport(factory.buildTransport(webSocket))
    }

    override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
        logger.debug("onFailure ${t.message}")
        handler.onFailure(t)
    }

    override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
        handler.onMessage(bytes.toByteArray())
    }

    override fun onClosing(webSocket: WebSocket, code: Int, reason: String) {
        handler.onClose()
    }
}
