package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.client.wss.WssDialer
import io.lantern.messaging.tassis.byteString
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import mu.KotlinLogging
import org.junit.Test
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.util.KeyHelper
import java.nio.ByteBuffer
import java.util.*
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testNoPreKeys() {
        runBlocking(Dispatchers.IO) {
            val scope = this
            val catIdentityCh = Channel<ECPublicKey>()

            launch {
                newMessaging(this).use { cat ->
                    catIdentityCh.send(cat.store.identityKeyPair.publicKey)
                }
            }
            launch {
                newMessaging(this).use { dog ->
                    val kp = KeyHelper.generateIdentityKeyPair()
                    val content = newContent("hello cat")
                    dog.send(Model.OutboundUserMessage.newBuilder()
                        .addRecipients(kp.publicKey.bytes.byteString())
                        .setContent(content).build())
                    val userMsg = waitFor(500000) {
                        val result = dog.store.db.get<Model.UserMessage>(content.dbPath)
                        result?.let { if (it.status == Model.DeliveryStatus.FAILING) it else null }
                    }
                    assertEquals(
                        Model.DeliveryStatus.FAILING,
                        userMsg?.status,
                        "attempt to send to cat with no known pre keys should have resulted in a UserMessage with failing status"
                    )
                    val catIdentity = catIdentityCh.receive()
                    dog.send(Model.OutboundUserMessage.newBuilder()
                        .addRecipients(catIdentity.bytes.byteString())
                        .setContent(content).build())
                }
            }
            logger.debug("test finished")
        }
        logger.debug("finished runBlocking")
    }

    private suspend fun <T> waitFor(maxWait: Int, get: suspend () -> T?): T? {
        var elapsed = 0
        while (elapsed < maxWait) {
            val result = get()
            if (result != null) {
                return result
            }
            delay(250)
            elapsed += 250
        }
        return null
    }

    private fun newContent(body: String): Model.UserMessageContent {
        return Model.UserMessageContent.newBuilder().setId(randomMessageId)
            .setSent(System.currentTimeMillis() * 1000000).setBody(body).build()
    }

    private val randomMessageId: ByteString
        get() {
            val uuid = UUID.randomUUID()
            val bb = ByteBuffer.wrap(ByteArray(16))
            bb.putLong(uuid.mostSignificantBits)
            bb.putLong(uuid.leastSignificantBits)
            return ByteString.copyFrom(bb.array())
        }

    private val dialer = WssDialer("wss://tassis.lantern.io/api")

    private suspend fun newMessaging(
        scope: CoroutineScope,
        numInitialPreKeysToRegister: Int = 5
    ): Messaging {
        val store = newStore
        return Messaging(
            store,
            scope,
            numInitialPreKeysToRegister = numInitialPreKeysToRegister,
            name=store.identityKeyPair.publicKey.toString()
        ) {
            dialer.dial(scope)
        }
    }
}