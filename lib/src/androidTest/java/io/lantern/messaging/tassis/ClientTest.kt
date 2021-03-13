package io.lantern.messaging.tassis

import androidx.test.ext.junit.runners.AndroidJUnit4
import io.lantern.messaging.BaseMessagingTest
import io.lantern.messaging.client.wss.WssDialer
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime

@RunWith(AndroidJUnit4::class)
@ExperimentalTime
class ClientTest : BaseMessagingTest() {
    @Test
    fun smokeTest() {
        val client1 = newClient
        runBlocking {
            val spk = client1.store.nextSignedPreKey
            val otpks = client1.store.generatePreKeys(5)
            client1.registerPreKeys(spk, otpks)
            assertTrue(true)
        }
    }

    private val newClient: Client
        get() = Client(
            newStore,
            WssDialer("wss://tassis.lantern.io/api"),
            GlobalScope)
}

