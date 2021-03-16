package io.lantern.messaging.client.wss

import io.ktor.client.*
import io.ktor.client.features.websocket.*
import io.ktor.http.cio.websocket.*
import io.ktor.http.cio.websocket.Frame.*
import io.lantern.messaging.tassis.ClientConnection
import io.lantern.messaging.tassis.Dialer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import mu.KotlinLogging

internal val logger = KotlinLogging.logger {}

class WssDialer(private val url: String, pingIntervalMillis: Long = 2500) : Dialer {
    private val client = HttpClient {
        install(WebSockets) {
            pingInterval = pingIntervalMillis
        }
    }

    override suspend fun dial(scope: CoroutineScope): ClientConnection {
        val conn = WssConnection(client, url)
        conn.start(scope)
        return conn
    }
}

class WssConnection(private val client: HttpClient, private val url: String) : ClientConnection {
    @Volatile
    private var job: Job? = null

    override val outbound = Channel<ByteArray>(100)
    override val inbound = Channel<ByteArray>(100)

    suspend fun start(scope: CoroutineScope) {
        job = scope.launch {
            client.ws(url) {
                try {
                    while (scope.isActive) {
                        select<Unit> {
                            outbound.onReceive {
                                send(Binary(true, it))
                            }
                            incoming.onReceive {
                                inbound.send(it.readBytes())
                            }
                        }
                    }
                } catch (t: Throwable) {
                    logger.error("error communicating with tassis: ${t.message}", t)
                    return@ws
                }
            }
            logger.debug("wss connection done processing")
        }
    }

    override fun close() {
        job?.cancel()
        client.close()
    }
}