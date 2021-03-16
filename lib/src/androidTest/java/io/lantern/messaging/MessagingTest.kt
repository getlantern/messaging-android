package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.tassis.byteString
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
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
        runBlocking {
            val catIdentityCh = Channel<ECPublicKey>()

            launch(Dispatchers.IO) {
                newMessaging(0).use { cat ->
                    catIdentityCh.send(cat.store.identityKeyPair.publicKey)
                }
            }

            newMessaging(5).use { dog ->
                val kp = KeyHelper.generateIdentityKeyPair()
                val content = newContent("hello cat")
                dog.send(
                    Model.OutboundUserMessage.newBuilder()
                        .addRecipients(kp.publicKey.bytes.byteString())
                        .setContent(content).build()
                )
                val userMsg = waitFor(5000) {
                    val result = dog.store.db.get<Model.UserMessage>(content.dbPath)
                    result?.let { if (it.status == Model.DeliveryStatus.FAILING) it else null }
                }
                assertEquals(
                    Model.DeliveryStatus.FAILING,
                    userMsg?.status,
                    "attempt to send to cat with no known pre keys should have resulted in a UserMessage with failing status"
                )
                val catIdentity = catIdentityCh.receive()
                dog.send(
                    Model.OutboundUserMessage.newBuilder()
                        .addRecipients(catIdentity.bytes.byteString())
                        .setContent(content).build()
                )
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

    private fun newMessaging(numInitialPreKeysToRegister: Int): Messaging {
        val store = newStore
        return Messaging(
            store,
            WebSocketTransportFactory("wss://tassis.lantern.io/api"),
            numInitialPreKeysToRegister = numInitialPreKeysToRegister,
            name = store.identityKeyPair.publicKey.toString()
        )
    }
}