package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.db.DB
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.websocket.WebSocketTransport
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import io.lantern.messaging.time.millisToSeconds
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.secondsToMillis
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
import org.whispersystems.libsignal.util.KeyHelper
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import java.io.ByteArrayInputStream
import java.io.File
import java.nio.charset.Charset
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testCompleteFlow() {
        val catStore = newStore()
        val theDog = newMessaging("dog")

        testInCoroutine {
            theDog.with { it ->
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
                    "dog->cat only sends successfully once cat registers pre keys",
                    dog,
                    catStore,
                    "hello cat",
                    attachments = arrayOf(
                        dog.createAttachment(
                            "text/plain", ByteArrayInputStream(
                                "attachment for cat".toByteArray(
                                    Charset.defaultCharset()
                                )
                            )
                        )
                    )
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
                    assertEquals(Model.MessageDirection.OUT, storedMsgRecord.direction)
                    assertEquals(msgRecord.ts, storedMsgRecord.ts)
                    assertEquals(
                        "hello cat",
                        runBlocking {
                            Model.ShortMessage.parseFrom(storedMsgRecord!!.message).text
                        }
                    )

                    logger.debug("close dog")
                    dog.close()
                    logger.debug("before reopening dog, set dials to fail for a while")
                    BrokenTransportFactory.succeedDialing.set(false)
                    GlobalScope.launch {
                        delay(2000)
                        logger.debug("allow dials to succeed again")
                        BrokenTransportFactory.succeedDialing.set(true)
                    }
                    logger.debug("reopen dog to make sure we can pick up where we left off")
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
                    BrokenTransportFactory.closeAll()

                    // now try sending again from dog
                    sendAndVerifyReceived<Any>(
                        "cat receives message after adding dog as contact",
                        dog,
                        cat,
                        text = "hello again cat",
                        attachments = arrayOf(
                            dog.createAttachment(
                                "text/plain", ByteArrayInputStream(
                                    "new attachment for cat".toByteArray(
                                        Charset.defaultCharset()
                                    )
                                )
                            )
                        )
                    )

                    // now reply from cat
                    val mostRecentMsg = cat.db.list<Model.ShortMessageRecord>(
                        Schema.PATH_MESSAGES.path("%"),
                        count = 1,
                        reverseSort = true
                    ).first().value
                    sendAndVerifyReceived<Any>(
                        "cat can successfully respond to dog",
                        cat,
                        dog,
                        "hi dog",
                        replyToId = mostRecentMsg.id
                    )

                    dog.unregister()
                    cat.unregister()
                }
            }
            logger.debug("test finished")

        }
        logger.debug("finished runBlocking")
    }

    @Test
    fun testDeliveryStatus() {
        testInCoroutine {
            newMessaging("cat").with { cat ->
                newMessaging("dog", stopSendRetryAfterMillis = 5000).with { dog ->
                    val catId = cat.store.identityKeyPair.publicKey.toString()
                    val dogId = dog.store.identityKeyPair.publicKey.toString()
                    val fakeId = KeyHelper.generateIdentityKeyPair().publicKey.toString()

                    cat.addOrUpdateDirectContact(dogId, "Dog")
                    dog.addOrUpdateDirectContact(catId, "Cat")
                    dog.addOrUpdateDirectContact(fakeId, "Fake")

                    val msg1 = dog.sendToDirectContact(catId, "hi cat")
                    assertNotNull(dog.waitFor<Model.ShortMessageRecord>(msg1.dbPath) {
                        it?.status == Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_SENT
                    }, "sending to real recipient should have succeeded")

                    val msg2 = dog.sendToDirectContact(fakeId, "hi fake one")
                    assertNotNull(dog.waitFor<Model.ShortMessageRecord>(msg2.dbPath) {
                        it?.status == Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_FAILED
                    }, "sending to fake recipient should have failed")

                    assertEquals(
                        0,
                        dog.db.list<Any>(Schema.PATH_OUTBOUND.path("%")).size,
                        "there should be no queued outbound messages once deliveries have succeeded and failed"
                    )
                }
            }
        }
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
        text: String?,
        attachments: Array<Model.StoredAttachment>? = null,
        replyToId: String? = null,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return sendAndVerifyReceived(
            testCase,
            from,
            to.store,
            text,
            attachments,
            replyToId = replyToId,
            afterSend = afterSend
        )
    }

    private suspend fun <T> sendAndVerifyReceived(
        testCase: String,
        from: Messaging,
        to: MessagingStore,
        text: String?,
        attachments: Array<Model.StoredAttachment>? = null,
        replyToId: String? = null,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return doSendAndVerify(testCase, from, to, text, attachments, true, replyToId, afterSend)
    }

    private suspend fun <T> sendAndVerifyDropped(
        testCase: String,
        from: Messaging,
        to: MessagingStore,
        text: String?,
        attachments: Array<Model.StoredAttachment>? = null,
        replyToId: String? = null,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        return doSendAndVerify(testCase, from, to, text, attachments, false, replyToId, afterSend)
    }

    private suspend fun <T> doSendAndVerify(
        testCase: String,
        from: Messaging,
        to: MessagingStore,
        text: String?,
        attachments: Array<Model.StoredAttachment>? = null,
        expectDelivery: Boolean,
        replyToId: String? = null,
        afterSend: (suspend (msgRecord: Model.ShortMessageRecord) -> T)? = null
    ): T? {
        logger.debug("running case $testCase")
        val fromId = from.store.identityKeyPair.publicKey.toString()
        val toId = to.identityKeyPair.publicKey.toString()

        logger.debug("ignore sends for a while to make sure client handles this well")
        BrokenTransportFactory.ignoreSends.set(true)
        GlobalScope.launch {
            delay(2000)
            logger.debug("start honoring sends again")
            BrokenTransportFactory.ignoreSends.set(false)
        }

        // send a message
        val msgRecord = from.sendToDirectContact(
            toId,
            text,
            attachments = attachments,
            replyToId = replyToId,
            replyToSenderId = replyToId?.let { toId })
        assertFalse(msgRecord.id.isNullOrBlank())
        assertEquals(Model.ShortMessageRecord.DeliveryStatus.SENDING, msgRecord.status, testCase)
        assertEquals(Model.MessageDirection.OUT, msgRecord.direction, testCase)
        assertEquals(fromId, msgRecord.senderId, testCase)
        assertTrue(msgRecord.ts > 0, testCase)
        assertTrue(msgRecord.ts < nowUnixNano, testCase)
        if (replyToId != null) {
            assertEquals(toId, msgRecord.replyToSenderId)
            assertEquals(replyToId, msgRecord.replyToId)
        }
        assertEquals(text, runBlocking {
            Model.ShortMessage.parseFrom(msgRecord.message).text
        }, testCase)

        // make sure the contact has been updated and that there's only one index entry
        var storedContact =
            from.db.get<Model.Contact>(toId.directContactPath)
        assertTrue(storedContact != null, testCase)
        assertEquals(toId, storedContact.id, testCase)
        assertEquals(msgRecord.ts, storedContact.mostRecentMessageTs, testCase)
        assertEquals(msgRecord.direction, storedContact.mostRecentMessageDirection, testCase)
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

        if (attachments != null) {
            if (attachments.size > 0) {
                assertEquals(attachments.size, msgRecord.attachmentsCount)
                assertEquals(attachments.size, msgRecord.attachmentStatusCount)
                attachments.forEach { attachment ->
                    assertNotNull(msgRecord.attachmentsMap.values.find { it.guid == attachment.guid })
                    assertEquals(
                        File(attachment.filePath).length(),
                        AttachmentCipherOutputStream.getCiphertextLength(attachment.attachment.plaintextLength)
                    )
                }
            }
        }

        val result = afterSend?.let { it(msgRecord) }

        // ensure that recipient has received the message
        val storedMsgRecord =
            to.waitFor<Model.ShortMessageRecord>(msgRecord.timestampUnknownQuery) { it != null }
        if (!expectDelivery) {
            assertNull(storedMsgRecord, testCase)
        } else {
            assertTrue(storedMsgRecord != null, testCase)
            assertEquals(msgRecord.id, storedMsgRecord.id)
            assertEquals(Model.MessageDirection.IN, storedMsgRecord.direction, testCase)
            assertEquals(fromId, storedMsgRecord.senderId, testCase)
            assertTrue(msgRecord.ts < storedMsgRecord.ts, testCase)
            assertEquals(Model.MessageDirection.IN, storedMsgRecord.direction)
            if (replyToId != null) {
                assertEquals(toId, storedMsgRecord.replyToSenderId)
                assertEquals(replyToId, storedMsgRecord.replyToId)
            }

            assertEquals(text, runBlocking {
                Model.ShortMessage.parseFrom(storedMsgRecord.message).text
            }, testCase)

            // ensure that recipient has the conversation too
            storedContact =
                to.db.get(fromId.directContactPath)
            assertTrue(storedContact != null, testCase)
            assertEquals(fromId, storedContact.id, testCase)
            assertEquals(storedMsgRecord.ts, storedContact.mostRecentMessageTs, testCase)
            assertEquals(
                storedMsgRecord.direction,
                storedContact.mostRecentMessageDirection,
                testCase
            )
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
                to.db.get(storedMsgRecord.contactMessagePath(storedContact)),
                testCase
            )
        }
        return result
    }

    private fun newMessaging(
        name: String,
        clientTimeoutMillis: Long = 5L.secondsToMillis,
        failedSendRetryDelayMillis: Long = 100,
        stopSendRetryAfterMillis: Long = 5L.minutesToMillis,
        store: MessagingStore? = null
    ): Messaging {
        return Messaging(
            File(
                InstrumentationRegistry.getInstrumentation().targetContext.filesDir,
                "attachments"
            ),
            store ?: newStore(name = name),
            BrokenTransportFactory(
                "wss://tassis.lantern.io/api",
                (clientTimeoutMillis / 2).millisToSeconds.toInt()
            ),
            clientTimeoutMillis = clientTimeoutMillis,
            redialBackoffMillis = 50L,
            maxRedialDelayMillis = 500L,
            failedSendRetryDelayMillis = failedSendRetryDelayMillis,
            stopSendRetryAfterMillis = stopSendRetryAfterMillis,
            name = name
        )
    }
}

@ExperimentalTime
internal suspend fun <T : Any> Messaging.waitFor(
    path: String,
    duration: Duration = 30.seconds,
    check: (T?) -> Boolean
): T? {
    return this.store.waitFor(path, duration, check)
}

@ExperimentalTime
internal suspend fun <T : Any> MessagingStore.waitFor(
    path: String,
    duration: Duration = 30.seconds,
    check: (T?) -> Boolean
): T? {
    return waitFor(duration.toLongMilliseconds()) {
        val result: T? = this.db.findOne(path)
        if (check(result)) result else null
    }
}

private suspend fun <T> waitFor(maxWait: Long, get: suspend () -> T?): T? {
    var elapsed = 0
    while (elapsed < maxWait) {
        val result = get()
        if (result != null) {
            logger.debug("waited ${elapsed}ms to find result")
            return result
        }
        delay(25)
        elapsed += 25
    }
    logger.debug("waited ${elapsed}ms without finding result")
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

internal class BrokenTransportFactory(url: String, connectionLostTimeoutSeconds: Int) :
    WebSocketTransportFactory(url, connectionLostTimeoutSeconds = connectionLostTimeoutSeconds) {
    override fun buildTransport(
        url: String,
        handler: MessageHandler,
        connectTimeoutMillis: Int,
        connectionLostTimeoutSeconds: Int
    ): WebSocketTransport {
        val transport = BrokenTransport(
            if (succeedDialing.get()) url else "wss://unknownbaddomain.lantern.io", // use a bad url to mimic a dial failure
            handler,
            connectTimeoutMillis,
            connectionLostTimeoutSeconds
        )
        addTransport(transport)
        return transport
    }

    companion object {
        var succeedDialing = AtomicBoolean(true)
        val ignoreSends = AtomicBoolean(false)
        private val transports = ArrayList<BrokenTransport>()

        @Synchronized
        fun addTransport(transport: BrokenTransport) {
            transports.add(transport)
        }

        @Synchronized
        fun removeTransport(transport: BrokenTransport) {
            transports.remove(transport)
        }

        @Synchronized
        fun closeAll() {
            transports.forEach { it.forceClose(); }
            transports.clear()
        }
    }
}

internal class BrokenTransport(
    url: String,
    handler: MessageHandler,
    connectTimeoutMillis: Int,
    connectionLostTimeoutSeconds: Int
) : WebSocketTransport(url, handler, connectTimeoutMillis, connectionLostTimeoutSeconds) {
    override fun send(data: ByteArray) {
        if (BrokenTransportFactory.ignoreSends.get()) {
            return
        }
        super.send(data)
    }

    override fun sendPing() {
        if (BrokenTransportFactory.ignoreSends.get()) {
            return
        }
        super.sendPing()
    }

    override fun onClose(code: Int, reason: String?, remote: Boolean) {
        Thread {
            BrokenTransportFactory.removeTransport(this)
        }.start()
        super.doOnClose(code, reason, remote)
    }
}