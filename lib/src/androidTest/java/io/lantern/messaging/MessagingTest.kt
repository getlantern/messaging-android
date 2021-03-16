package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.client.wss.WssDialer
import io.lantern.messaging.tassis.byteString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
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
//            newMessaging(scope).use { cat ->
            newMessaging(scope).use { dog ->
                val content = newContent("hello cat")
                val kp = KeyHelper.generateIdentityKeyPair()
                val msg = Model.OutboundUserMessage.newBuilder()
//                        .addRecipients(cat.store.identityKeyPair.publicKey.bytes.byteString())
                    .addRecipients(kp.publicKey.bytes.byteString())
                    .setContent(content).build()
                dog.send(msg)
                val userMsg = waitFor(15000) {
                    val result = dog.store.db.get<Model.UserMessage>(msg.content.dbPath)
                    result?.let { if (it.status == Model.DeliveryStatus.FAILING) it else null }
                }
                assertEquals(
                    Model.DeliveryStatus.FAILING,
                    userMsg?.status,
                    "attempt to send to cat with no known pre keys should have resulted in a UserMessage with failing status"
                )
                logger.debug("test finished")
            }
//            }
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
        return Messaging(
            newStore,
            scope,
            numInitialPreKeysToRegister = numInitialPreKeysToRegister
        ) {
            dialer.dial(scope)
        }
    }
}