package io.lantern.messaging

import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
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

        testInCoroutine {
            dog.use { dog ->
                val catId = catStore.identityKeyPair.publicKey.toString()
                val dogId = dog.store.identityKeyPair.publicKey.toString()

                // first add Cat as a contact
                dog.addOrUpdateContact(catId, "Cat")
                // ensure that we immediately have a conversation
                assertTrue(dog.db.findOne<Model.Conversation>(catId.contactConversationQuery) != null)
                val storedContact = dog.db.get<Model.Contact>(catId.contactPath)
                assertTrue(storedContact != null)
                assertEquals(catId, storedContact.id)
                assertEquals("Cat", storedContact.displayName)

                // now send a message from dog->cat before cat has come online
                // we do not expect this message to be delivered to cat because cat hasn't added dog
                // as a contact
                val cat = sendAndVerifyDropped(dog, catStore, "hello cat") { msgRecord ->
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
                    assertEquals(
                        "hello cat",
                        Model.ShortMessage.parseFrom(storedMsgRecord?.message).text
                    )

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

                // now have cat add dog as a contact
                cat.addOrUpdateContact(dogId, "Dog")

                // now try sending again from dog
                sendAndVerifyReceived<Any>(dog, cat, "hello again cat")

                // now respond from cat
                sendAndVerifyReceived<Any>(cat, dog, "hi dog")

                dog.unregister()
                cat.unregister()
            }
            logger.debug("test finished")

        }
        logger.debug("finished runBlocking")
    }

    private fun testInCoroutine(fn: suspend () -> Unit) {
        var thrown: Throwable? = null
        runBlocking {
            try {
                fn()
            } catch (t: Throwable) {
                thrown = t
            }
        }
        thrown?.let { throw it }
    }

    private suspend fun <T> sendAndVerifyReceived(
        from: Messaging,
        to: Messaging,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return sendAndVerifyReceived(from, to.store, text)
    }

    private suspend fun <T> sendAndVerifyDropped(
        from: Messaging,
        to: Messaging,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return sendAndVerifyDropped(from, to.store, text)
    }

    private suspend fun <T> sendAndVerifyReceived(
        from: Messaging,
        to: MessagingStore,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return doSendAndVerify(from, to, text, true, afterSend)
    }

    private suspend fun <T> sendAndVerifyDropped(
        from: Messaging,
        to: MessagingStore,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return doSendAndVerify(from, to, text, false, afterSend)
    }

    private suspend fun <T> doSendAndVerify(
        from: Messaging,
        to: MessagingStore,
        text: String,
        expectDelivery: Boolean,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        val fromId = from.store.identityKeyPair.publicKey.toString()
        val toId = to.identityKeyPair.publicKey.toString()

        // send a message
        val msgRecord = from.sendToContact(toId, text)
        assertEquals(Model.ShortMessageRecord.DeliveryStatus.UNSENT, msgRecord.status)
        assertEquals(Model.ShortMessageRecord.Direction.OUT, msgRecord.direction)
        assertEquals(fromId, msgRecord.senderId)
        assertTrue(msgRecord.sent < nowUnixNano)
        assertEquals(text, Model.ShortMessage.parseFrom(msgRecord.message).text)

        // make sure the conversation has been created or updated and that there's only one
        // conversation entry for this contact
        var storedConversation =
            from.db.get<Model.Conversation>(toId.contactConversationPath(msgRecord.sent))
        assertTrue(storedConversation != null)
        assertEquals(toId, storedConversation.contactId)
        assertTrue(storedConversation.groupId == "")
        assertEquals(msgRecord.sent, storedConversation.mostRecentMessageTime)
        assertEquals(text, storedConversation.mostRecentMessageText)
        assertTrue(from.db.findOne<Model.Conversation>(toId.contactConversationQuery) != null)

        // make sure that there's a link to the message in sender's conversation messages
        assertEquals(
            msgRecord.dbPath,
            from.db.get(msgRecord.conversationMessagePath(storedConversation))
        )

        val result = afterSend?.let { it(msgRecord) }

        // ensure that recipient has received the message
        val storedMsgRecord =
            to.waitFor<Model.ShortMessageRecord>(msgRecord.dbPath) { it != null }
        if (!expectDelivery) {
            assertNull(storedMsgRecord)
        } else {
            assertTrue(storedMsgRecord != null)
            assertEquals(Model.ShortMessageRecord.Direction.IN, storedMsgRecord.direction)
            assertEquals(fromId, storedMsgRecord.senderId)
            assertEquals(msgRecord.sent, storedMsgRecord.sent)
            assertEquals(text, Model.ShortMessage.parseFrom(storedMsgRecord.message).text)

            // ensure that recipient has the conversation too
            storedConversation = to.db.get(fromId.contactConversationPath(msgRecord.sent))
            assertTrue(storedConversation != null)
            assertEquals(fromId, storedConversation.contactId)
            assertTrue(storedConversation.groupId == "")
            assertEquals(msgRecord.sent, storedConversation.mostRecentMessageTime)
            assertEquals(text, storedConversation.mostRecentMessageText)
            assertTrue(to.db.findOne<Model.Conversation>(fromId.contactConversationQuery) != null)

            // make sure that there's a link to the message in recipient's conversation messages
            assertEquals(
                storedMsgRecord.dbPath,
                to.db.get(msgRecord.conversationMessagePath(storedConversation))
            )
        }
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