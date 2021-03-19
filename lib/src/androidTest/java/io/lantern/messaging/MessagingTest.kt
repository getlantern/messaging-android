package io.lantern.messaging

import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testCompleteFlow() {
        val catStore = newStore
        val dog = newMessaging("dog")

        runBlocking {
            dog.use { dog ->
                // first send a message from dog->cat before cat has come online
                val cat = send(dog, catStore, "hello cat") { msgRecord ->
                    // wait for the message to attempt to send and fail, make sure the status is correct
                    // and that the message is still populated correctly
                    var storedMsgRecord = dog.waitFor<Model.ShortMessageRecord>(msgRecord.dbPath) {
                        it?.status == Model.ShortMessageRecord.DeliveryStatus.FAILING
                    }
                    assertTrue(storedMsgRecord != null)
                    assertEquals(
                        Model.ShortMessageRecord.DeliveryStatus.FAILING,
                        storedMsgRecord?.status,
                        "attempt to send to cat before cat has started registering preKeys should have resulted in a UserMessage with failing status"
                    )
                    assertEquals(Model.ShortMessageRecord.Direction.OUT, storedMsgRecord?.direction)
                    assertEquals(msgRecord.sent, storedMsgRecord?.sent)
                    assertEquals("hello cat", Model.ShortMessage.parseFrom(storedMsgRecord?.message).text)

                    // start the Messaging system for cat, which will result in the registration of pre
                    // keys, allowing the message to send successfully
                    val cat = newMessaging("cat", store = catStore)
                    storedMsgRecord =
                        dog.waitFor(msgRecord.dbPath) { it?.status == Model.ShortMessageRecord.DeliveryStatus.SENT }
                    assertTrue(storedMsgRecord != null)
                    assertEquals(
                        Model.ShortMessageRecord.DeliveryStatus.SENT,
                        storedMsgRecord?.status,
                        "once cat has started registering preKeys, pending message should successfully send"
                    )
                    cat
                }

                assertTrue(cat != null)

                // now respond from cat
                send<Any>(cat, dog, "hi dog")

                dog.unregister()
                cat.unregister()
            }
            logger.debug("test finished")

        }
        logger.debug("finished runBlocking")
    }

    private suspend fun <T> send(from: Messaging, to: Messaging, text: String, afterSend: (suspend (msgRecord: Model.ShortMessageRecord)->T)? = null): T? {
        return send(from, to.store, text)
    }

    private suspend fun <T> send(from: Messaging, to: MessagingStore, text: String, afterSend: (suspend (msgRecord: Model.ShortMessageRecord)->T)? = null): T? {
        val fromId = from.store.identityKeyPair.publicKey.toString()
        val toId = to.identityKeyPair.publicKey.toString()

        // send a message
        val msgRecord = from.send(text, contactId = toId)
        assertEquals(Model.ShortMessageRecord.DeliveryStatus.UNSENT, msgRecord.status)
        assertEquals(Model.ShortMessageRecord.Direction.OUT, msgRecord.direction)
        assertEquals(fromId, msgRecord.senderId)
        assertTrue(msgRecord.sent < nowUnixNano)
        assertEquals(text, Model.ShortMessage.parseFrom(msgRecord.message).text)

        // make sure the conversation has been created
        var storedConversation =
            from.store.db.get<Model.Conversation>(msgRecord.conversationPath(toId))
        assertTrue(storedConversation != null)
        assertEquals(toId, storedConversation.contactId)
        assertTrue(storedConversation.groupId == "")
        assertEquals(msgRecord.sent, storedConversation.mostRecentMessageTime)
        assertEquals(text, storedConversation.mostRecentMessageText)

        // make sure that there's a link to the message in sender's conversation messages
        assertEquals(
            msgRecord.dbPath,
            from.store.db.get(msgRecord.conversationMessagePath(storedConversation))
        )

        val result = afterSend?.let { it(msgRecord) }

        // ensure that recipient has received the message
        val storedMsgRecord =
            to.waitFor<Model.ShortMessageRecord>(msgRecord.dbPath) { it != null }
        assertTrue(storedMsgRecord != null)
        assertEquals(Model.ShortMessageRecord.Direction.IN, storedMsgRecord.direction)
        assertEquals(fromId, storedMsgRecord.senderId)
        assertEquals(msgRecord.sent, storedMsgRecord.sent)
        assertEquals(text, Model.ShortMessage.parseFrom(storedMsgRecord.message).text)

        // ensure that recipient has the conversation too
        storedConversation = to.db.get(msgRecord.conversationPath(fromId))
        assertTrue(storedConversation != null)
        assertEquals(fromId, storedConversation.contactId)
        assertTrue(storedConversation.groupId == "")
        assertEquals(msgRecord.sent, storedConversation.mostRecentMessageTime)
        assertEquals(text, storedConversation.mostRecentMessageText)

        // make sure that there's a link to the message in recipient's conversation messages
        assertEquals(
            storedMsgRecord.dbPath,
            to.db.get(msgRecord.conversationMessagePath(storedConversation))
        )

        return result
    }

    private fun newMessaging(
        name: String,
        failedSendRetryDelay: Duration = 100.milliseconds,
        store: MessagingStore? = null
    ): Messaging {
        return Messaging(
            store ?: newStore,
            WebSocketTransportFactory("wss://tassis.lantern.io/api"),
            failedSendRetryDelay = failedSendRetryDelay,
            name = name
        )
    }
}

@ExperimentalTime
internal suspend fun <T : Any> Messaging.waitFor(
    path: String,
    duration: Duration = 5.seconds,
    check: (T?) -> Boolean
): T? {
    return this.store.waitFor(path, duration, check)
}

@ExperimentalTime
internal suspend fun <T : Any> MessagingStore.waitFor(
    path: String,
    duration: Duration = 5.seconds,
    check: (T?) -> Boolean
): T? {
    return waitFor(duration.toLongMilliseconds()) {
        val result: T? = this.db.get(path)
        if (check(result)) result else null
    }
}

private suspend fun <T> waitFor(maxWait: Long, get: suspend () -> T?): T? {
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