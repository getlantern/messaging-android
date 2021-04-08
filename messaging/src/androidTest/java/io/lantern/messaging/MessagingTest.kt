package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.db.DB
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import io.lantern.messaging.tassis.websocket.WSListener
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.secondsToMillis
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import org.junit.Test
import org.whispersystems.libsignal.util.KeyHelper
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import org.whispersystems.signalservice.internal.util.Util
import java.io.ByteArrayInputStream
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.*
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testBasicFlowWithConnectivityIssues() {
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

                // try to create an overly large attachment and make sure it fails
                try {
                    dog.createAttachment(
                        "application/octet-stream",
                        Long.MAX_VALUE - AttachmentCipherOutputStream.MAXIMUM_ENCRYPTION_OVERHEAD,
                        ByteArrayInputStream(ByteArray(0))
                    )
                    fail("creating a giant attachment shouldn't be allowed")
                } catch (e: AttachmentTooBigException) {
                    assertTrue(e.maxAttachmentBytes > 0)
                    assertTrue(e.maxAttachmentBytes < Long.MAX_VALUE)
                }

                // first add Cat as a contact
                dog.addOrUpdateContact(Model.ContactType.DIRECT, catId, "Cat")
                // ensure that we immediately have a Contact
                val storedContact = dog.db.get<Model.Contact>(catId.directContactPath)
                assertTrue(storedContact != null)
                assertEquals(catId, storedContact.contactId.id)
                assertEquals("Cat", storedContact.displayName)

                // now send a message from dog->cat before cat has come online
                // we do not expect this message to be delivered to cat because cat hasn't added dog
                // as a contact
                val sentMsg = dog.sendToDirectContact(
                    catId, "hello cat", attachments = arrayOf(
                        dog.createAttachment(
                            "text/plain",
                            "attachment for cat".length.toLong(),
                            ByteArrayInputStream(
                                "attachment for cat".toByteArray(
                                    Charsets.UTF_8
                                )
                            )
                        )
                    )
                )
                var sentMsgFromDB = dog.waitFor<Model.StoredMessage>(sentMsg.dbPath) {
                    it?.status == Model.StoredMessage.DeliveryStatus.SENDING
                }
                assertTrue(sentMsgFromDB != null, "message should still be in SENDING status")

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
                newMessaging("cat", store = catStore).with { cat ->
                    val sentMsgFromDB =
                        dog.waitFor<Model.StoredMessage>(sentMsg.dbPath) { it?.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT }
                    assertNotNull(sentMsgFromDB, "message from dog to cat should have successfully sent once Cat registered pre keys")
                    assertEquals(
                        Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT,
                        sentMsgFromDB.status,
                        "once cat has started registering preKeys, pending message should successfully send"
                    )

                    // now have cat add dog as a contact
                    cat.addOrUpdateContact(Model.ContactType.DIRECT, dogId, "Dog")

                    // verify that cat now has message from dog
                    val mostRecentMsg =
                        cat.waitFor<Model.StoredMessage>(Schema.PATH_MESSAGES.path("%")) {
                            it?.attachmentsMap?.filter { (_, attachment) -> attachment.status == Model.StoredAttachment.Status.DONE }
                                ?.count() ?: 0 == 1
                        }
                    assertNotNull(mostRecentMsg, "cat should have recent message")
                    assertEquals(
                        dogId,
                        mostRecentMsg.senderId,
                        "most recent message should have come from dog"
                    )
                    assertEquals(
                        "hello cat",
                        mostRecentMsg.text,
                        "most recent message should have had correct text"
                    )

                    // close connections to make sure reconnecting works okay
                    BrokenTransportFactory.closeAll()

                    // now reply from cat
                    sendAndVerify(
                        "cat can successfully respond to dog",
                        cat,
                        dog,
                        "hi dog",
                        replyToId = mostRecentMsg.id
                    )

                    // locally delete most recent message that cat received from dog
                    cat.deleteLocally(mostRecentMsg.dbPath)
                    assertNull(
                        cat.db.get<Model.StoredMessage>(mostRecentMsg.dbPath),
                        "message should have been deleted"
                    )
                    mostRecentMsg.attachmentsMap.values.forEach { storedAttachment ->
                        assertFalse(
                            File(storedAttachment.filePath).exists(),
                            "attachment file should have been deleted"
                        )
                    }

                    // globally delete most recent message that cat sent to dog
                    cat.db.list<Model.StoredMessage>(
                        Schema.PATH_MESSAGES.path("%"),
                        count = 1,
                        reverseSort = true
                    ).first().let { responseMsg ->
                        cat.deleteGlobally(responseMsg.path)
                        assertNull(
                            cat.db.get<Model.StoredMessage>(responseMsg.path),
                            "message should have been deleted locally"
                        )
                        val dogContact =
                            cat.db.get<Model.Contact>(responseMsg.value.contactId.contactPath)
                        assertNotNull(dogContact, "cat should still have a contact for dog")
                        assertEquals(
                            0L,
                            dogContact.mostRecentMessageTs,
                            "dog contact should have no most recent message timestamp"
                        )
                        assertEquals(
                            "",
                            dogContact.mostRecentMessageText,
                            "dog contact should have no most recent message text"
                        )
                        assertEquals(
                            "",
                            dogContact.mostRecentAttachmentMimeType,
                            "dog contact should have no most recent message attachment mime type"
                        )
                        val dogMsg =
                            dog.waitFor<Model.StoredMessage>(catId.storedMessageQuery(responseMsg.value.id)) {
                                it == null
                            }
                        assertNull(dogMsg, "message should have been deleted for dog too")
                    }


                    // make sure outbound and inbound queues are empty
                    assertEquals(
                        0,
                        dog.db.list<Any>(Schema.PATH_OUTBOUND.path("%")).size,
                        "dog should have no queued outbound messages"
                    )
                    assertEquals(
                        0,
                        dog.db.list<Any>(Schema.PATH_INBOUND_ATTACHMENTS.path("%")).size,
                        "dog should have no queued inbound attachments"
                    )

                    assertEquals(
                        0,
                        dog.db.list<Any>(Schema.PATH_OUTBOUND.path("%")).size,
                        "cat should have no queued outbound messages"
                    )
                    assertEquals(
                        0,
                        dog.db.list<Any>(Schema.PATH_INBOUND_ATTACHMENTS.path("%")).size,
                        "cat should have no queued inbound attachments"
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
    public fun testReactions() {
        testInCoroutine {
            newMessaging("dog").with { dog ->
                newMessaging("cat").with { cat ->
                    val catId = cat.identityKeyPair.publicKey.toString()
                    val dogId = dog.store.identityKeyPair.publicKey.toString()

                    dog.addOrUpdateContact(Model.ContactType.DIRECT, catId, "Cat")
                    cat.addOrUpdateContact(Model.ContactType.DIRECT, dogId, "Dog")

                    val msgs = sendAndVerify("dog sends to cat", dog, cat, "hi cat")
                    assertNotNull(msgs.received)

                    // add reaction from cat
                    cat.react(
                        msgs.received.dbPath,
                        "g"
                    ) // no, 'g' is not an emoticon, but it's just a test
                    var updatedMostRecentMsg = cat.db.get<Model.StoredMessage>(msgs.received.dbPath)
                    assertNotNull(updatedMostRecentMsg)
                    assertEquals(
                        "g",
                        updatedMostRecentMsg.getReactionsOrThrow(catId).emoticon,
                        "cat's reaction should have been recorded locally"
                    )

                    val dogMsg =
                        dog.waitFor<Model.StoredMessage>(msgs.sent.dbPath) {
                            it?.reactionsCount ?: 0 > 0
                        }
                    assertNotNull(dogMsg, "dog should have gotten reaction")
                    assertEquals(
                        "g",
                        dogMsg.getReactionsOrThrow(catId).emoticon,
                        "cat's reaction should have been recorded for dog too"
                    )

                    // clear reaction from cat
                    cat.react(
                        msgs.received.dbPath,
                        ""
                    )
                    updatedMostRecentMsg =
                        cat.db.get<Model.StoredMessage>(msgs.received.dbPath)
                    assertNotNull(updatedMostRecentMsg)
                    assertEquals(
                        0,
                        updatedMostRecentMsg.reactionsCount,
                        "cat's reaction should have been cleared"
                    )

                    val newerDogMsg =
                        dog.waitFor<Model.StoredMessage>(msgs.sent.dbPath) {
                            it?.reactionsCount ?: 0 == 0
                        }
                    assertNotNull(newerDogMsg, "dog should have cleared reaction")
                }
            }
        }
    }

    @Test
    fun testDeliveryStatus() {
        testInCoroutine {
            newMessaging("cat").with { cat ->
                newMessaging("dog", stopSendRetryAfterMillis = 5000).with { dog ->
                    val catId = cat.store.identityKeyPair.publicKey.toString()
                    val dogId = dog.store.identityKeyPair.publicKey.toString()
                    val fakeId = KeyHelper.generateIdentityKeyPair().publicKey.toString()

                    cat.addOrUpdateContact(Model.ContactType.DIRECT, dogId, "Dog")
                    dog.addOrUpdateContact(Model.ContactType.DIRECT, catId, "Cat")
                    dog.addOrUpdateContact(Model.ContactType.DIRECT, fakeId, "Fake")

                    val msg1 = dog.sendToDirectContact(catId, "hi cat")
                    assertNotNull(dog.waitFor<Model.StoredMessage>(msg1.dbPath) {
                        it?.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT
                    }, "sending to real recipient should have succeeded")

                    val msg2 = dog.sendToDirectContact(fakeId, "hi fake one")
                    assertNotNull(dog.waitFor<Model.StoredMessage>(msg2.dbPath) {
                        it?.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED
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

    class MessagePair(val sent: Model.StoredMessage, val received: Model.StoredMessage)

    private suspend fun sendAndVerify(
        testCase: String,
        from: Messaging,
        to: Messaging,
        text: String?,
        attachments: Array<Model.StoredAttachment>? = null,
        replyToId: String? = null,
        ignoreSendsForAWhile: Boolean = false
    ): MessagePair {
        logger.debug("running case $testCase")
        val fromId = from.store.identityKeyPair.publicKey.toString()
        val toId = to.identityKeyPair.publicKey.toString()

        if (ignoreSendsForAWhile) {
            logger.debug("ignore sends for a while to make sure client handles this well")
            BrokenTransportFactory.ignoreOps.set(true)
            GlobalScope.launch {
                delay(2000)
                logger.debug("start honoring sends again")
                BrokenTransportFactory.ignoreOps.set(false)
            }
        }

        // send a message
        val senderStoredMsg = from.sendToDirectContact(
            toId,
            text,
            attachments = attachments,
            replyToId = replyToId,
            replyToSenderId = replyToId?.let { toId })
        assertFalse(senderStoredMsg.id.isNullOrBlank())
        assertEquals(Model.StoredMessage.DeliveryStatus.SENDING, senderStoredMsg.status, testCase)
        assertEquals(Model.MessageDirection.OUT, senderStoredMsg.direction, testCase)
        assertEquals(fromId, senderStoredMsg.senderId, testCase)
        assertTrue(senderStoredMsg.ts > 0, testCase)
        assertTrue(senderStoredMsg.ts < nowUnixNano, testCase)
        if (replyToId != null) {
            assertEquals(toId, senderStoredMsg.replyToSenderId)
            assertEquals(replyToId, senderStoredMsg.replyToId)
        }
        assertEquals(text, senderStoredMsg.text, testCase)

        // make sure the contact has been updated and that there's only one index entry
        var storedContact =
            from.db.get<Model.Contact>(toId.directContactPath)
        assertTrue(storedContact != null, testCase)
        assertEquals(toId, storedContact.contactId.id, testCase)
        assertEquals(senderStoredMsg.ts, storedContact.mostRecentMessageTs, testCase)
        assertEquals(senderStoredMsg.direction, storedContact.mostRecentMessageDirection, testCase)
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
            senderStoredMsg.dbPath,
            from.db.get(senderStoredMsg.contactMessagePath),
            testCase
        )

        if (attachments != null) {
            assertEquals(attachments.size, senderStoredMsg.attachmentsCount, testCase)
            assertEquals(
                senderStoredMsg.attachmentsMap[0]!!.attachment.mimeType,
                storedContact.mostRecentAttachmentMimeType,
                testCase
            )
            attachments.forEach { attachment ->
                val storedAttachment =
                    senderStoredMsg.attachmentsMap.values.find { it.guid == attachment.guid }
                assertNotNull(storedAttachment, testCase)
                assertEquals(
                    attachment.attachment.metadataMap,
                    storedAttachment.attachment.metadataMap,
                    testCase
                )
                assertEquals(
                    File(attachment.filePath).length(),
                    AttachmentCipherOutputStream.getCiphertextLength(attachment.attachment.plaintextLength),
                    testCase
                )
            }
        }

        // ensure that recipient has received the message
        var recipientStoredMsg =
            to.waitFor<Model.StoredMessage>(senderStoredMsg.timestampUnknownQuery) { it != null }
        assertTrue(recipientStoredMsg != null, testCase)
        assertEquals(senderStoredMsg.id, recipientStoredMsg.id)
        assertEquals(Model.MessageDirection.IN, recipientStoredMsg.direction, testCase)
        assertEquals(fromId, recipientStoredMsg.senderId, testCase)
        assertTrue(senderStoredMsg.ts < recipientStoredMsg.ts, testCase)
        assertEquals(Model.MessageDirection.IN, recipientStoredMsg.direction)
        if (replyToId != null) {
            assertEquals(toId, recipientStoredMsg.replyToSenderId)
            assertEquals(replyToId, recipientStoredMsg.replyToId)
        }

        assertEquals(text, recipientStoredMsg.text, testCase)

        // ensure that recipient has the conversation too
        storedContact =
            to.db.get(fromId.directContactPath)
        assertTrue(storedContact != null, testCase)
        assertEquals(fromId, storedContact.contactId.id, testCase)
        assertEquals(recipientStoredMsg.ts, storedContact.mostRecentMessageTs, testCase)
        assertEquals(
            recipientStoredMsg.direction,
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
            recipientStoredMsg.dbPath,
            to.db.get(recipientStoredMsg.contactMessagePath),
            testCase
        )

        // make sure recipient got attachments
        if (attachments != null) {
            assertEquals(attachments.size, recipientStoredMsg.attachmentsCount, testCase)
            assertEquals(
                recipientStoredMsg.attachmentsMap[0]!!.attachment.mimeType,
                storedContact.mostRecentAttachmentMimeType,
                testCase
            )
            // wait for all attachments to download
            recipientStoredMsg =
                to.waitFor<Model.StoredMessage>(recipientStoredMsg.dbPath) {
                    it?.attachmentsMap?.values?.count { it.status != Model.StoredAttachment.Status.DONE } == 0
                }
            assertNotNull(recipientStoredMsg, testCase)
            recipientStoredMsg.attachmentsMap.forEach { (id, attachment) ->
                // make sure metadata matches expected
                assertEquals(
                    attachments[id].attachment.metadataMap,
                    attachment.attachment.metadataMap
                )
                // make sure decrypted content matches expected
                assertTrue(
                    Util.streamsEqual(
                        attachment.inputStream,
                        attachments[id].inputStream,
                    ), testCase
                )
            }
        }

        return MessagePair(senderStoredMsg, recipientStoredMsg)
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
                (clientTimeoutMillis / 2)
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
    }.joinToString("\n")
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

internal class BrokenTransportFactory(url: String, connectTimeoutMillis: Long) :
    WebSocketTransportFactory(url, connectTimeoutMillis = connectTimeoutMillis) {
    override fun getUrl(): String =
        if (succeedDialing.get()) super.getUrl() else "wss://badtassis.lantern.io:9436"

    override fun buildListener(
        handler: MessageHandler
    ): WebSocketListener = object : WSListener(this, handler) {
        override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
            if (ignoreOps.get()) {
                return
            }
            super.onMessage(webSocket, bytes)
        }
    }

    override fun buildTransport(webSocket: WebSocket): Transport {
        val wrapped = super.buildTransport(webSocket)

        val transport = object : Transport {
            override fun send(data: ByteArray) {
                if (ignoreOps.get()) {
                    return
                }
                wrapped.send(data)
            }

            override fun cancel() {
                Thread {
                    removeTransport(this)
                }.start()
                wrapped.cancel()
            }

            override fun close() {
                Thread {
                    removeTransport(this)
                }.start()
                if (ignoreOps.get()) {
                    return
                }
                wrapped.close()
            }
        }
        addTransport(transport)
        return transport
    }

    companion object {
        var succeedDialing = AtomicBoolean(true)
        val ignoreOps = AtomicBoolean(false)
        private val transports = ArrayList<Transport>()

        @Synchronized
        fun addTransport(transport: Transport) {
            transports.add(transport)
        }

        @Synchronized
        fun removeTransport(transport: Transport) {
            transports.remove(transport)
        }

        @Synchronized
        fun closeAll() {
            transports.forEach { it.cancel(); }
            transports.clear()
        }
    }
}