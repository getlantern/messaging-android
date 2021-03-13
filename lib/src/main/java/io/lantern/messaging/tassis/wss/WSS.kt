package io.lantern.messaging.client.wss

import io.ktor.client.*
import io.ktor.client.features.websocket.*
import io.ktor.http.cio.websocket.*
import io.ktor.http.cio.websocket.Frame.*
import io.lantern.messaging.tassis.ClientConnection
import io.lantern.messaging.tassis.Dialer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select

class WssDialer(private val url: String) : Dialer {
    private val client = HttpClient {
        install(WebSockets)
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
    override val errors =  Channel<Throwable>(100)

    suspend fun start(scope: CoroutineScope) {
        job = scope.launch {
            client.ws(url) {
                while (isActive) {
                    select<Unit> {
                        outbound.onReceive {
                            send(Binary(true, it))
                        }
                        incoming.onReceive {
                            inbound.send(it.readBytes())
                        }
                    }
                }
            }
        }
    }

    override fun close() {
        job?.cancel()
    }
}