package io.lantern.messaging.tassis.websocket

import androidx.test.ext.junit.runners.AndroidJUnit4
import io.lantern.messaging.ValueMonitor
import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.runner.RunWith

// this is a special byte sequence that tassis recognizes and closes the connection on its end
private val forceClose = "forceclose".toByteArray(Charsets.UTF_8)

@RunWith(AndroidJUnit4::class)
class WebSocketTransportTest {
    @Test
    fun testClosedLocally() {
        val factory =
            WebSocketTransportFactory("wss://tassis-test.lantern.io/api", connectTimeoutMillis = 5000)
        val handler = TestHandler()
        Thread {
            factory.connect(handler)
            val transport = handler.transport.get(5000)
            assertNotNull(transport, "handler should have gotten Transport")
            transport.close() // this will result in a proper websocket close handshake
        }.start()
        assertNotNull(handler.message.get(2000), "should have received message")
        assertTrue(handler.closed.get(2000), "handler should have been notified of close")
        assertNull(handler.failure.get(2000), "handler should not have received failure")
    }

    @Test
    fun testClosedRemotely() {
        val factory =
            WebSocketTransportFactory("wss://tassis-test.lantern.io/api", connectTimeoutMillis = 5000)
        val handler = TestHandler()
        Thread {
            factory.connect(handler)
            val transport = handler.transport.get(5000)
            assertNotNull(transport, "handler should have gotten Transport")
            transport.send(forceClose)
        }.start()
        assertNotNull(handler.message.get(2000), "should have received message")
        assertTrue(handler.closed.get(2000), "handler should have been notified of close")
        assertNull(handler.failure.get(2000), "handler should not have received failure")
    }

    @Test
    fun testCancel() {
        val factory =
            WebSocketTransportFactory("wss://tassis-test.lantern.io/api", connectTimeoutMillis = 5000)
        val handler = TestHandler()
        Thread {
            factory.connect(handler)
            val transport = handler.transport.get(5000)
            assertNotNull(transport, "handler should have gotten Transport")
            transport.cancel()
        }.start()
        assertNotNull(handler.message.get(2000), "should have received message")
        assertFalse(handler.closed.get(2000), "handler should not have been notified of close")
        assertNotNull(handler.failure.get(2000), "handler should have received failure")
    }

    @Test
    fun testConnectFailure() {
        val factory =
            WebSocketTransportFactory("wss://badtassis-test.lantern.io/api", connectTimeoutMillis = 5000)
        val handler = TestHandler()
        Thread {
            factory.connect(handler)
        }.start()
        assertNull(handler.transport.get(5000), "handler should not have gotten Transport")
        assertNotNull(handler.failure.get(2000), "handler should have been notified of failure")
    }

    @Test
    fun testReadTimeout() {
        val factory =
            WebSocketTransportFactory(
                "wss://badtassis-test.lantern.io/api",
                connectTimeoutMillis = 5000,
                readTimeoutMillis = 100
            )
        val handler = TestHandler()
        Thread {
            factory.connect(handler)
        }.start()
        assertNull(handler.transport.get(5000), "handler should not have gotten Transport")
        assertNotNull(handler.failure.get(2000), "handler should have been notified of failure")
    }
}

internal class TestHandler : MessageHandler {
    internal val transport = ValueMonitor<Transport?>(null)
    internal val failure = ValueMonitor<Throwable?>(null)
    internal val message = ValueMonitor<ByteArray?>(null)
    internal val closed = ValueMonitor(false)

    override fun setTransport(transport: Transport) {
        this.transport.set(transport)
    }

    override fun onFailure(err: Throwable) {
        failure.set(err)
    }

    override fun onMessage(data: ByteArray?) {
        message.set(data)
    }

    override fun onClose() {
        closed.set(true)
    }
}
