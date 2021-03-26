package io.lantern.messaging

import io.lantern.db.DB
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.websocket.WebSocketTransport
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testCompleteFlow() {
        val catStore = newStore()
        val dog = newMessaging("dog")

        testInCoroutine {
            dog.with { it ->
                var dog = it
                val catId = catStore.identityKeyPair.publicKey.toString()
                val dogId = dog.store.identityKeyPair.publicKey.toString()

                assertNotNull(
                    dog.store.db.get<Model.Contact>(Schema.PATH_ME),
                    "self-contact should exist"
                )
                dog.setMyDisplayName("I'm a Dog")
                assertEquals(
                    "I'm a Dog",
                    dog.store.db.get<Model.Contact>(Schema.PATH_ME)?.displayName
                )

                // first add Cat as a contact
                dog.addOrUpdateDirectContact(catId, "Cat")
                // ensure that we immediately have a Contact
                val storedContact = dog.db.get<Model.Contact>(catId.directContactPath)
                assertTrue(storedContact != null)
                assertEquals(catId, storedContact.id)
                assertEquals("Cat", storedContact.displayName)

                // now send a message from dog->cat before cat has come online
                // we do not expect this message to be delivered to cat because cat hasn't added dog
                // as a contact
                val cat = sendAndVerifyDropped(
                    "dog->cat only sends successfully one cat registers pre keys",
                    dog,
                    catStore,
                    "hello cat"
                ) { msgRecord ->
                    // wait for the message to attempt to send and fail, make sure the status is correct
                    // and that the message is still populated correctly
                    var storedMsgRecord = dog.waitFor<Model.ShortMessageRecord>(msgRecord.dbPath) {
                        it?.status == Model.ShortMessageRecord.DeliveryStatus.SENDING
                    }
                    assertTrue(storedMsgRecord != null)
                    assertEquals(
                        Model.ShortMessageRecord.DeliveryStatus.SENDING,
                        storedMsgRecord.status,
                        "attempt to send to cat before cat has started registering preKeys should have resulted in a UserMessage with failing status"
                    )
                    assertEquals(Model.ShortMessageRecord.Direction.OUT, storedMsgRecord.direction)
                    assertEquals(msgRecord.sent, storedMsgRecord.sent)
                    assertEquals(
                        "hello cat",
                        runBlocking {
                            Model.ShortMessage.parseFrom(storedMsgRecord!!.message).text
                        }
                    )

                    // Close and reopen dog to make sure we can pick up where we left off
                    dog.close()
                    dog = newMessaging("dog")

                    // start the Messaging system for cat, which will result in the registration of pre
                    // keys, allowing the message to send successfully
                    val cat = newMessaging("cat", store = catStore)
                    storedMsgRecord =
                        dog.waitFor(msgRecord.dbPath) { it?.status == Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_SENT }
                    assertTrue(storedMsgRecord != null)
                    assertEquals(
                        Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_SENT,
                        storedMsgRecord.status,
                        "once cat has started registering preKeys, pending message should successfully send"
                    )
                    cat
                }
                assertTrue(cat != null)

                cat.with {
                    // now have cat add dog as a contact
                    cat.addOrUpdateDirectContact(dogId, "Dog")

                    // close connections to make sure reconnecting works okay
                    MisbehavingTransportFactory.closeAll()

                    // now try sending again from dog
                    sendAndVerifyReceived<Any>(
                        "cat receives message after adding dog as contact",
                        dog,
                        cat,
                        "hello again cat"
                    )

                    // now respond from cat
                    sendAndVerifyReceived<Any>(
                        "cat can successfully respond to dog",
                        cat,
                        dog,
                        "hi dog"
                    )

                    dog.unregister()
                    cat.unregister()
                }
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
        testCase: String,
        from: Messaging,
        to: Messaging,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return sendAndVerifyReceived(testCase, from, to.store, text)
    }

    private suspend fun <T> sendAndVerifyReceived(
        testCase: String,
        from: Messaging,
        to: MessagingStore,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return doSendAndVerify(testCase, from, to, text, true, afterSend)
    }

    private suspend fun <T> sendAndVerifyDropped(
        testCase: String,
        from: Messaging,
        to: MessagingStore,
        text: String,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return doSendAndVerify(testCase, from, to, text, false, afterSend)
    }

    private suspend fun <T> doSendAndVerify(
        testCase: String,
        from: Messaging,
        to: MessagingStore,
        text: String,
        expectDelivery: Boolean,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        logger.debug("running case $testCase")
        val fromId = from.store.identityKeyPair.publicKey.toString()
        val toId = to.identityKeyPair.publicKey.toString()

        // send a message
        val msgRecord = from.sendToDirectContact(toId, text)
        assertEquals(Model.ShortMessageRecord.DeliveryStatus.SENDING, msgRecord.status, testCase)
        assertEquals(Model.ShortMessageRecord.Direction.OUT, msgRecord.direction, testCase)
        assertEquals(fromId, msgRecord.senderId, testCase)
        assertTrue(msgRecord.sent < nowUnixNano, testCase)
        assertEquals(text, runBlocking {
            Model.ShortMessage.parseFrom(msgRecord.message).text
        }, testCase)

        // make sure the contact has been updated and that there's only one index entry
        var storedContact =
            from.db.get<Model.Contact>(toId.directContactPath)
        assertTrue(storedContact != null, testCase)
        assertEquals(toId, storedContact.id, testCase)
        assertEquals(msgRecord.sent, storedContact.mostRecentMessageTime, testCase)
        assertEquals(text, storedContact.mostRecentMessageText, testCase)
        assertEquals(
            1,
            from.db.listDetails<Model.Contact>(
                Schema.PATH_CONTACTS_BY_ACTIVITY.path(
                    "%",
                    storedContact.pathSegment
                )
            ).size,
            testCase
        )

        // make sure that there's a link to the message in sender's contact messages
        assertEquals(
            msgRecord.dbPath,
            from.db.get(msgRecord.contactMessagePath(storedContact)),
            testCase
        )

        val result = afterSend?.let { it(msgRecord) }

        // ensure that recipient has received the message
        val storedMsgRecord =
            to.waitFor<Model.ShortMessageRecord>(msgRecord.dbPath) { it != null }
        if (!expectDelivery) {
            assertNull(storedMsgRecord, testCase)
        } else {
            assertTrue(storedMsgRecord != null, testCase)
            assertEquals(Model.ShortMessageRecord.Direction.IN, storedMsgRecord.direction, testCase)
            assertEquals(fromId, storedMsgRecord.senderId, testCase)
            assertEquals(msgRecord.sent, storedMsgRecord.sent, testCase)
            assertEquals(text, runBlocking {
                Model.ShortMessage.parseFrom(storedMsgRecord.message).text
            }, testCase)

            // ensure that recipient has the conversation too
            storedContact =
                to.db.get(fromId.directContactPath)
            assertTrue(storedContact != null, testCase)
            assertEquals(fromId, storedContact.id, testCase)
            assertEquals(msgRecord.sent, storedContact.mostRecentMessageTime, testCase)
            assertEquals(text, storedContact.mostRecentMessageText, testCase)
            assertEquals(
                1,
                to.db.listDetails<Model.Contact>(
                    Schema.PATH_CONTACTS_BY_ACTIVITY.path(
                        "%",
                        storedContact.pathSegment
                    )
                ).size,
                testCase
            )


            // make sure that there's a link to the message in recipient's contact messages
            assertEquals(
                storedMsgRecord.dbPath,
                to.db.get(msgRecord.contactMessagePath(storedContact)),
                testCase
            )
        }
        return result
    }

    private fun newMessaging(
        name: String,
        failedSendRetryDelayMillis: Long = 100,
        store: MessagingStore? = null
    ): Messaging {
        return Messaging(
            store ?: newStore(name = name),
            MisbehavingTransportFactory("wss://tassis.lantern.io/api"),
            failedSendRetryDelayMillis = failedSendRetryDelayMillis,
            name = name
        )
    }
}

@ExperimentalTime
internal suspend fun <T : Any> Messaging.waitFor(
    path: String,
    duration: Duration = 10.seconds,
    check: (T?) -> Boolean
): T? {
    return this.store.waitFor(path, duration, check)
}

@ExperimentalTime
internal suspend fun <T : Any> MessagingStore.waitFor(
    path: String,
    duration: Duration = 10.seconds,
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
        delay(25)
        elapsed += 25
    }
    return null
}

internal fun DB.dump() {
    val dumpString = this.list<Any>("%").sortedBy { it.path }.map {
        "${it.path}: ${it.value}"
    }
    println("DB Dump for ${this.get<Model.Contact>(Schema.PATH_ME)?.displayName}\n===============================================\n\n${dumpString}\n\n======================================")
}

internal suspend fun Messaging.with(fn: suspend (messaging: Messaging) -> Unit) = this.use {
    try {
        fn(this)
    } catch (t: Throwable) {
        try {
            this.store.db.dump()
        } catch (t: Throwable) {
            // ignore
        }
        throw t
    }
}

internal class MisbehavingTransportFactory(url: String) : WebSocketTransportFactory(url) {
    override fun buildTransport(
        url: String,
        handler: MessageHandler,
        connectTimeoutMillis: Int,
        connectionLostTimeoutSeconds: Int
    ): WebSocketTransport {
        // Connect a little slowly
        val transport = MisbehavingTransport(
            url,
            handler,
            connectTimeoutMillis,
            connectionLostTimeoutSeconds
        )
        addTransport(transport)
        return transport
    }

    companion object {
        private val transports = ArrayList<MisbehavingTransport>()

        @Synchronized
        fun addTransport(transport: MisbehavingTransport) {
            transports.add(transport)
        }

        @Synchronized
        fun removeTransport(transport: MisbehavingTransport) {
            transports.remove(transport)
        }

        @Synchronized
        fun closeAll() {
            transports.forEach { it.closeBlocking() }
            transports.clear()
        }
    }
}

internal class MisbehavingTransport(
    url: String,
    handler: MessageHandler,
    connectTimeoutMillis: Int,
    connectionLostTimeoutSeconds: Int
) : WebSocketTransport(url, handler, connectTimeoutMillis, connectionLostTimeoutSeconds) {
    override fun onClose(code: Int, reason: String?, remote: Boolean) {
        Thread {
            MisbehavingTransportFactory.removeTransport(this)
        }.start()
        super.doOnClose(code, reason, remote)
    }
}