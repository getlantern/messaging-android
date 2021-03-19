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
                val dogId = dog.store.identityKeyPair.publicKey.bytes.base32
                val catId = catStore.identityKeyPair.publicKey.bytes.base32
                val text = "hello cat"
                val responseText = "hi dog"

                // send a message and make sure it's populated correctly
                val msgRecord = dog.send(text, contactId = catId)
                assertEquals(Model.ShortMessageRecord.DeliveryStatus.UNSENT, msgRecord.status)
                assertEquals(Model.ShortMessageRecord.Direction.OUT, msgRecord.direction)
                assertEquals(dogId, msgRecord.senderId)
                assertTrue(msgRecord.sent < nowUnixNano)
                assertEquals(text, Model.ShortMessage.parseFrom(msgRecord.message).text)

                // make sure the conversation has been created
                var storedConversation =
                    dog.store.db.get<Model.Conversation>(msgRecord.conversationPath(catId))
                assertTrue(storedConversation != null)
                assertEquals(catId, storedConversation.contactId)
                assertTrue(storedConversation.groupId == "")
                assertEquals(msgRecord.sent, storedConversation.mostRecentMessageTime)
                assertEquals(text, storedConversation.mostRecentMessageText)

                // make sure that there's a link to the message in conversation messages
                assertEquals(
                    msgRecord.dbPath,
                    dog.store.db.get(msgRecord.conversationMessagePath(storedConversation))
                )

                // wait for the message to attempt to send and fail, make sure the status is correct
                // and that the message is still populated correctly
                var storedMsgRecord = dog.waitFor<Model.ShortMessageRecord>(msgRecord.dbPath) {
                    it?.status == Model.ShortMessageRecord.DeliveryStatus.FAILING
                }
                assertTrue(storedMsgRecord != null)
                assertEquals(
                    Model.ShortMessageRecord.DeliveryStatus.FAILING,
                    storedMsgRecord.status,
                    "attempt to send to cat before cat has started registering preKeys should have resulted in a UserMessage with failing status"
                )
                assertEquals(Model.ShortMessageRecord.Direction.OUT, storedMsgRecord.direction)
                assertEquals(
                    dogId, storedMsgRecord.senderId
                )
                assertEquals(msgRecord.sent, storedMsgRecord.sent)
                assertEquals(text, Model.ShortMessage.parseFrom(storedMsgRecord.message).text)

                // start the Messaging system for cat, which will result in the registration of pre
                // keys, allowing the message to send successfully
                val cat = newMessaging("cat", store = catStore)
                storedMsgRecord =
                    dog.waitFor(msgRecord.dbPath) { it?.status == Model.ShortMessageRecord.DeliveryStatus.SENT }
                assertTrue(storedMsgRecord != null)
                assertEquals(
                    Model.ShortMessageRecord.DeliveryStatus.SENT,
                    storedMsgRecord.status,
                    "once cat has started registering preKeys, pending UserMessage should successfully send"
                )
                assertEquals(Model.ShortMessageRecord.Direction.OUT, storedMsgRecord.direction)
                assertEquals(dogId, storedMsgRecord.senderId)
                assertEquals(msgRecord.sent, storedMsgRecord.sent)
                assertEquals(text, Model.ShortMessage.parseFrom(storedMsgRecord.message).text)

                // ensure that cat has received the message
                storedMsgRecord =
                    cat.waitFor(msgRecord.dbPath) { it != null }
                assertTrue(storedMsgRecord != null)
                assertEquals(Model.ShortMessageRecord.Direction.IN, storedMsgRecord.direction)
                assertEquals(dogId, storedMsgRecord.senderId)
                assertEquals(msgRecord.sent, storedMsgRecord.sent)
                assertEquals(text, Model.ShortMessage.parseFrom(storedMsgRecord.message).text)

                // ensure that cat has the conversation too
                storedConversation = cat.store.db.get(msgRecord.conversationPath(dogId))
                assertTrue(storedConversation != null)
                assertEquals(dogId, storedConversation.contactId)
                assertTrue(storedConversation.groupId == "")
                assertEquals(msgRecord.sent, storedConversation.mostRecentMessageTime)
                assertEquals(text, storedConversation.mostRecentMessageText)

                // make sure that there's a link to the message in conversation messages
                assertEquals(
                    storedMsgRecord.dbPath,
                    cat.store.db.get(msgRecord.conversationMessagePath(storedConversation))
                )

                // now respond from cat
                val responseMsgRecord = cat.send(responseText, contactId = dogId)
                assertEquals(Model.ShortMessageRecord.DeliveryStatus.UNSENT, responseMsgRecord.status)
                assertEquals(Model.ShortMessageRecord.Direction.OUT, responseMsgRecord.direction)
                assertEquals(catId, responseMsgRecord.senderId)
                assertTrue(responseMsgRecord.sent < nowUnixNano)
                assertEquals(responseText, Model.ShortMessage.parseFrom(responseMsgRecord.message).text)

                // make sure the conversation has been created
                storedConversation =
                    cat.store.db.get(responseMsgRecord.conversationPath(dogId))
                assertTrue(storedConversation != null)
                assertEquals(dogId, storedConversation.contactId)
                assertTrue(storedConversation.groupId == "")
                assertEquals(responseMsgRecord.sent, storedConversation.mostRecentMessageTime)
                assertEquals(responseText, storedConversation.mostRecentMessageText)

                // make sure that there's a link to the message in conversation messages
                assertEquals(
                    responseMsgRecord.dbPath,
                    cat.store.db.get(responseMsgRecord.conversationMessagePath(storedConversation))
                )

                // ensure that dog has received the message
                storedMsgRecord =
                    dog.waitFor(responseMsgRecord.dbPath) { it != null }
                assertTrue(storedMsgRecord != null)
                assertEquals(Model.ShortMessageRecord.Direction.IN, storedMsgRecord.direction)
                assertEquals(catId, storedMsgRecord.senderId)
                assertEquals(responseMsgRecord.sent, storedMsgRecord.sent)
                assertEquals(responseText, Model.ShortMessage.parseFrom(storedMsgRecord.message).text)

                // ensure that dog has the conversation too
                storedConversation = dog.store.db.get(responseMsgRecord.conversationPath(catId))
                assertTrue(storedConversation != null)
                assertEquals(catId, storedConversation.contactId)
                assertTrue(storedConversation.groupId == "")
                assertEquals(responseMsgRecord.sent, storedConversation.mostRecentMessageTime)
                assertEquals(responseText, storedConversation.mostRecentMessageText)

                // make sure that there's a link to the message in conversation messages
                assertEquals(
                    storedMsgRecord.dbPath,
                    dog.store.db.get(responseMsgRecord.conversationMessagePath(storedConversation))
                )

                dog.unregister()
                cat.unregister()
            }
            logger.debug("test finished")

        }
        logger.debug("finished runBlocking")
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
    return waitFor(duration.toLongMilliseconds()) {
        val result: T? = this.store.db.get(path)
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