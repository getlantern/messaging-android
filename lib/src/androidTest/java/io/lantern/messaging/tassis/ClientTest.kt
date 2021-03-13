package io.lantern.messaging.tassis

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.messaging.BaseMessagingTest
import io.lantern.messaging.client.wss.WssDialer
import io.lantern.messaging.store.MessagingStore
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import java.nio.file.Paths
import kotlin.test.assertTrue
import kotlin.time.ExperimentalTime

@RunWith(AndroidJUnit4::class)
@ExperimentalTime
class ClientTest : BaseMessagingTest() {
    @Test
    fun smokeTest() {
        runBlocking {
            val client1 = newClient
            val spk = client1.store.nextSignedPreKey
            val otpks = client1.store.generatePreKeys(5)
            client1.registerPreKeys(spk, otpks)
            cancel()
        }
    }

    private val newClient: Client
        get() = Client(
            newStore,
            WssDialer("wss://tassis.lantern.io/api"),
            GlobalScope)
}

