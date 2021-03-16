package io.lantern.tassis

import io.lantern.messaging.BaseMessagingTest
import io.lantern.messaging.client.wss.WssDialer
import io.lantern.messaging.tassis.Client
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.util.KeyHelper

class ClientTest : BaseMessagingTest() {
    @Test
    fun testClose() {
        runBlocking {
            val scope = this
            val job = scope.launch {
                dialer.dial(scope).use { anonymousConn ->
                    dialer.dial(scope).use { authenticatedConn ->
                        val kp = KeyHelper.generateIdentityKeyPair()
                        val client = Client(
                            kp.publicKey,
                            DeviceId.random(),
                            anonymousConn,
                            authenticatedConn,
                            scope
                        ) {
                            it
                        }
                    }
                }
            }
            job.cancel()
        }
    }

    private val dialer = WssDialer("wss://tassis.lantern.io/api")
}