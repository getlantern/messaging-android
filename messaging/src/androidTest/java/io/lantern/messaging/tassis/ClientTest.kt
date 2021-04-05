package io.lantern.messaging.tassis

import androidx.test.ext.junit.runners.AndroidJUnit4
import io.lantern.messaging.ValueMonitor
import io.lantern.messaging.tassis.websocket.WSTransport
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import okhttp3.WebSocket
import org.junit.runner.RunWith
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.util.KeyHelper
import java.util.concurrent.TimeoutException
import kotlin.test.Test
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

@RunWith(AndroidJUnit4::class)
class ClientTest {
    @Test
    fun testClosedLocally() {
        val factory =
            WebSocketTransportFactory("wss://tassis.lantern.io/api", connectTimeoutMillis = 5000)
        val delegate = Delegate()
        val client = AnonymousClient(delegate, 5000)
        Thread {
            factory.connect(client)
            assertTrue(delegate.connected.get(5000), "client should have connected")
            client.close()
        }.start()
        assertTrue(delegate.closed.get(10000))
    }

    @Test
    fun testCloseTimeout() {
        val factory =
            object : WebSocketTransportFactory(
                "wss://tassis.lantern.io/api",
                connectTimeoutMillis = 5000,
                callTimeoutMillis = 1000
            ) {
                override fun buildTransport(
                    webSocket: WebSocket
                ): Transport {
                    return object : WSTransport(webSocket) {
                        override fun close() {
                            // ignore close to make sure timeout kicks in
                        }
                    }
                }
            }
        val delegate = Delegate()
        val client = AnonymousClient(delegate, 5000)
        Thread {
            factory.connect(client)
            assertTrue(delegate.connected.get(5000), "client should have connected")
            client.close()
        }.start()
        assertTrue(delegate.closed.get(10000), "client should have closed")
    }

    @Test
    fun testConnectFailure() {
        val factory =
            WebSocketTransportFactory("wss://badtassis.lantern.io/api", connectTimeoutMillis = 5000)
        val delegate = Delegate()
        val client = AnonymousClient(delegate, 5000)
        Thread {
            factory.connect(client)
        }.start()
        assertNotNull(delegate.connectError.get(5000), "client should have failed to connect")
    }

    @Test
    fun testRoundTripTimeout() {
        val factory =
            object : WebSocketTransportFactory(
                "wss://tassis.lantern.io/api",
                connectTimeoutMillis = 5000
            ) {
                override fun buildTransport(
                    webSocket: WebSocket
                ): Transport {
                    return object : WSTransport(webSocket) {
                        override fun send(data: ByteArray) {
                            // don't send the data so that we get a timeout
                        }
                    }
                }
            }
        val delegate = Delegate()
        val client = AnonymousClient(delegate, 5000)
        Thread {
            factory.connect(client)
            assertTrue(delegate.connected.get(5000), "client should have connected")
            client.sendUnidentifiedSenderMessage(
                SignalProtocolAddress(
                    KeyHelper.generateIdentityKeyPair().publicKey,
                    DeviceId.random()
                ), ByteArray(0), object : Callback<Unit> {
                    override fun onSuccess(result: Unit) {
                        fail("call shouldn't have succeeded")
                    }

                    override fun onError(err: Throwable) {
                        assertTrue(err is TimeoutException)
                    }
                })
        }.start()
        assertTrue(delegate.closed.get(10000), "client should have closed")
    }
}

internal class Delegate : AnonymousClientDelegate {
    internal val connected = ValueMonitor(false)
    internal val connectError = ValueMonitor<Throwable?>(null)
    internal val configUpdated = ValueMonitor(false)
    internal val closed = ValueMonitor(false)

    override fun onConnected(client: AnonymousClient) {
        connected.set(true)
    }

    override fun onConnectError(err: Throwable) {
        connectError.set(err)
    }

    override fun onConfigUpdate(cfg: Messages.Configuration) {
        configUpdated.set(true)
    }

    override fun onClose(err: Throwable?) {
        closed.set(true)
    }
}
