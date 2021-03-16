package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.byteString
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
import java.nio.ByteBuffer
import java.util.*
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testCompleteFlow() {
        val catStore = newStore
        val dog = newMessaging("dog")

        runBlocking {
            dog.use { dog ->
                val catIdentity = catStore.identityKeyPair.publicKey
                val content = newContent("hello cat")
                dog.send(
                    Model.OutboundUserMessage.newBuilder()
                        .addRecipients(catIdentity.bytes.byteString())
                        .setContent(content).build()
                )
                var userMsg = waitFor(5000) {
                    val result = dog.store.db.get<Model.UserMessage>(content.dbPath)
                    result?.let { if (it.status == Model.DeliveryStatus.FAILING) it else null }
                }
                assertEquals(
                    Model.DeliveryStatus.FAILING,
                    userMsg?.status,
                    "attempt to send to cat before cat has started registering preKeys should have resulted in a UserMessage with failing status"
                )

                val cat = newMessaging("cat", store=catStore)
                userMsg = waitFor(5000) {
                    val result = dog.store.db.get<Model.UserMessage>(content.dbPath)
                    result?.let { if (it.status == Model.DeliveryStatus.SENT) it else null }
                }
                assertEquals(
                    Model.DeliveryStatus.SENT,
                    userMsg?.status,
                    "once cat has started registering preKeys, pending UserMessage should successfully send"
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

    private fun newMessaging(name: String, failedSendRetryDelay: Duration = 100.milliseconds, store: MessagingStore? = null): Messaging {
        return Messaging(
            store ?: newStore,
            WebSocketTransportFactory("wss://tassis.lantern.io/api"),
            failedSendRetryDelay=failedSendRetryDelay,
            name = name
        )
    }
}