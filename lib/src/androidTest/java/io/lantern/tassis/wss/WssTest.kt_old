package io.lantern.tassis.wss

import io.lantern.messaging.client.wss.WssDialer
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.Test

class WssTest {
    @Test
    fun testClose() {
        runBlocking {
            val conn = dialer.dial(this)
            conn.close()
        }
    }

    private val dialer = WssDialer("wss://tassis.lantern.io/api")
}