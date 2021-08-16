package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.db.DB
import io.lantern.messaging.tassis.MessageHandler
import io.lantern.messaging.tassis.Transport
import io.lantern.messaging.tassis.websocket.WSListener
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.secondsToMillis
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileOutputStream
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.ArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.seconds
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

private val logger = KotlinLogging.logger {}

private const val smileyFace = "\uD83D\uDE04"

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testManageDirectContact() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val dogId = dog.myId.id
                            val catId = cat.myId.id

                            val now = now
                            var catContact =
                                dog.addOrUpdateDirectContact(catId, "Cat")
                            val createdTs = catContact.createdTs
                            assertEquals(
                                Model.ContactType.DIRECT,
                                catContact.contactId.type,
                                "cat should have right contact type"
                            )
                            assertEquals(
                                catId,
                                catContact.contactId.id,
                                "cat id should have been set correctly"
                            )
                            assertEquals(
                                "Cat",
                                catContact.displayName,
                                "displayName should have been set"
                            )
                            assertTrue(createdTs >= now, "createdTime should have been set")

                            catContact = dog.addOrUpdateDirectContact(catId, "New Cat")
                            assertEquals(
                                "New Cat",
                                catContact.displayName,
                                "displayName should have been changed"
                            )
                            assertEquals(
                                createdTs,
                                catContact.createdTs,
                                "createdTime should have been left alone"
                            )

                            cat.addOrUpdateDirectContact(dogId, "Dog")
                            sendAndVerify(
                                "cat sends a message to dog",
                                cat,
                                dog,
                                "hi dog"
                            )
                            sendAndVerify(
                                "dog sends a message to cat",
                                dog,
                                cat,
                                "hi cat"
                            )
                            val msgs = sendAndVerify(
                                "cat sends another message to dog",
                                cat,
                                dog,
                                "hello again dog"
                            )

                            dog.deleteDirectContact(catId)
                            assertFalse(dog.db.contains(catId.directContactId.contactPath))
                            assertFalse(dog.db.contains(msgs.received.dbPath))
                            assertEquals(
                                0,
                                dog.db.listPaths(Schema.PATH_CONTACT_MESSAGES.path("%")).count()
                            )
                            assertEquals(
                                0,
                                dog.db.listPaths(Schema.PATH_CONTACTS.path("%")).count()
                            )
                            assertEquals(
                                0,
                                dog.db.listPaths(Schema.PATH_CONTACTS_BY_ACTIVITY.path("%")).count()
                            )
                            val dogDbString = dog.db.dumpToString()
                            assertFalse(
                                dogDbString.contains(catId),
                                "dog's db should have no mention of cat's ID"
                            )
                            val dogStoreDbString = dog.store.db.dumpToString()
                            assertFalse(
                                dogStoreDbString.contains(catId),
                                "dog's MessagingProtocolStore.db should have no mention of cat's ID"
                            )

                            cat.sendToDirectContact(dogId, "cat sent this while not a contact")

                            // hack dog's disappear settings to make sure we get hello message with
                            // new settings
                            cat.db.mutate { tx ->
                                tx.get<Model.Contact>(dogId.directContactPath)?.let {
                                    tx.put(
                                        it.dbPath,
                                        it.toBuilder().setMessagesDisappearAfterSeconds(17).build()
                                    )
                                }
                            }
                            dog.addOrUpdateDirectContact(catId, "New Cat")
                            cat.waitFor<Model.Contact>(
                                dogId.directContactPath,
                                "dog's initial disappear settings should arrive"
                            ) {
                                it.messagesDisappearAfterSeconds == 86400
                            }

                            sendAndVerify(
                                "cat sends a message to dog after having been removed and re-added",
                                cat,
                                dog,
                                "hello again dog"
                            )

                            assertEquals(
                                1,
                                dog.db.listPaths(Schema.PATH_CONTACT_MESSAGES.path("%")).count(),
                                "dog should have only 1 message from cat, the message sent while cat was not a contact should have been lost because it couldn't be decrypted" // ktlint-disable max-line-length
                            )
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testBasicFlowWithConnectivityIssues() {
        newDB.use { dogDB ->
            newDB.use { catDB ->
                val catStore = newStore(catDB)
                val theDog = newMessaging(dogDB, "dog")

                testInCoroutine {
                    theDog.with { it ->
                        var dog = it
                        val catId = catStore.identityKeyPair.publicKey.toString()
                        val dogId = dog.store.identityKeyPair.publicKey.toString()

                        assertNotNull(
                            dog.db.get<Model.Contact>(Schema.PATH_ME),
                            "self-contact should exist"
                        )
                        dog.setMyDisplayName("I'm a Dog")
                        assertEquals(
                            "I'm a Dog",
                            dog.db.get<Model.Contact>(Schema.PATH_ME)?.displayName
                        )

                        val hugeLength = Long.MAX_VALUE -
                            AttachmentCipherOutputStream.MAXIMUM_ENCRYPTION_OVERHEAD
                        // try to create an overly large attachment and make sure it fails
                        try {
                            dog.createAttachment(
                                "application/octet-stream",
                                hugeLength,
                                ByteArrayInputStream(ByteArray(0))
                            )
                            fail("creating a giant attachment shouldn't be allowed")
                        } catch (e: AttachmentTooBigException) {
                            assertTrue(e.maxAttachmentBytes > 0)
                            assertTrue(e.maxAttachmentBytes < Long.MAX_VALUE)
                        }

                        // first add Cat as a contact
                        dog.addOrUpdateDirectContact(catId, "Cat")
                        // ensure that we immediately have a Contact
                        val storedContact = dog.db.get<Model.Contact>(catId.directContactPath)
                        assertTrue(storedContact != null)
                        assertEquals(catId, storedContact.contactId.id)
                        assertEquals("Cat", storedContact.displayName)

                        // now send a message from dog->cat before cat has come online
                        // because cat hasn't yet added dog as a contact, this will first go to spam
                        // but once cat adds dog as a contact, cat should get the message
                        val input = ByteArrayInputStream(
                            "attachment for cat".toByteArray(
                                Charsets.UTF_8
                            )
                        )
                        // use a lazy attachment
                        val plainTextFile = File(tempDir, UUID.randomUUID().toString())
                        FileOutputStream(plainTextFile).use { output -> Util.copy(input, output) }
                        val attachment = dog.createAttachment(
                            plainTextFile,
                            "text/plain"
                        )
                        val sentMsg = dog.sendToDirectContact(
                            catId, "hello cat", attachments = arrayOf(attachment)
                        )
                        dog.waitFor<Model.StoredMessage>(
                            sentMsg.dbPath,
                            "message should still be in SENDING status"
                        ) {
                            it.status == Model.StoredMessage.DeliveryStatus.SENDING
                        }

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
                        dog = newMessaging(dogDB, "dog")

                        // start the Messaging system for cat, which will result in the registration of pre
                        // keys, allowing the message to send successfully
                        newMessaging(catDB, "cat").with { cat ->
                            val sentMsgFromDB =
                                dog.waitFor<Model.StoredMessage>(
                                    sentMsg.dbPath,
                                    "message from dog to cat should have successfully sent once Cat registered pre keys" // ktlint-disable max-line-length
                                ) {
                                    it.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT
                                }
                            assertEquals(
                                Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT,
                                sentMsgFromDB.status,
                                "once cat has started registering preKeys, pending message should successfully send" // ktlint-disable max-line-length
                            )

                            // now have cat add dog as a contact
                            cat.addOrUpdateDirectContact(dogId, "Dog")

                            // verify that cat now has message from dog
                            val mostRecentMsg =
                                cat.waitFor<Model.StoredMessage>(
                                    Schema.PATH_MESSAGES.path("%"),
                                    "cat should have recent message"
                                ) {
                                    it.attachmentsMap
                                        .filter { (_, attachment) ->
                                            attachment.status == Model.StoredAttachment.Status.DONE
                                        }.count() == 1
                                }
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
                                replyToId = mostRecentMsg.id,
                                ignoreSendsForMillis = 2000
                            )

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
            }
        }
    }

    @Test
    fun testResend() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    // use a relatively short stopSendRetryAfterMillis to make sure message fails
                    // in a reasonable amount of time
                    newMessaging(
                        dogDB,
                        "dog",
                        stopSendRetryAfterMillis = 5L.secondsToMillis
                    ).with { dog ->
                        val catStore = newStore(catDB)
                        val dogId = dog.myId.id
                        val catId = catStore.identityKeyPair.publicKey.toString()

                        dog.addOrUpdateDirectContact(catId, "Cat")
                        // Immediately try to send message before cat's messaging system has come
                        // online. At this point, there are not registered pre-keys for cat, so
                        // sending will fail.
                        val sentMsg = dog.sendToDirectContact(catId, "hello cat")
                        val failedMsg = dog.waitFor<Model.StoredMessage>(
                            sentMsg.dbPath,
                            "initial send should fail"
                        ) {
                            it.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED
                        }

                        newMessaging(catDB, "cat").with { cat ->
                            cat.addOrUpdateDirectContact(dogId, "Dog")

                            // now that cat has come online, we should get pre-keys, so try to
                            // resend
                            dog.resendFailedMessage(failedMsg.id)
                            dog.waitFor<Model.StoredMessage>(
                                sentMsg.dbPath,
                                "resend should succeed"
                            ) {
                                it.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT
                            }

                            val recvMsg = cat.waitFor<Model.StoredMessage>(
                                sentMsg.dbPath,
                                "cat should receive message"
                            )

                            try {
                                cat.resendFailedMessage("fakemessageid")
                                fail("resending non-existing message should fail")
                            } catch (e: java.lang.IllegalArgumentException) {
                                assertEquals("Message not found", e.message)
                            }

                            try {
                                cat.resendFailedMessage(recvMsg.id)
                                fail("resending message that wasn't sent by us should fail")
                            } catch (e: java.lang.IllegalArgumentException) {
                                assertEquals("Message not found", e.message)
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testAttachments() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val dogId = dog.myId.id
                            val catId = cat.myId.id
                            dog.addOrUpdateDirectContact(catId, "Cat")
                            cat.addOrUpdateDirectContact(dogId, "Dog")
                            // lazy attachment to a non-existent file
                            val badPlainTextFile = File(tempDir, UUID.randomUUID().toString())
                            val badAttachment = dog.createAttachment(
                                badPlainTextFile,
                                "text/plain"
                            )
                            val lazyPlainTextFile = File(tempDir, UUID.randomUUID().toString())
                            FileOutputStream(lazyPlainTextFile).use { output ->
                                Util.copy(
                                    ByteArrayInputStream(
                                        "lazy attachment".toByteArray(
                                            Charsets.UTF_8
                                        )
                                    ),
                                    output
                                )
                            }
                            val lazyAttachment = dog.createAttachment(
                                lazyPlainTextFile,
                                "text/plain"
                            )
                            val eagerPlainTextFile = File(tempDir, UUID.randomUUID().toString())
                            FileOutputStream(eagerPlainTextFile).use { output ->
                                Util.copy(
                                    ByteArrayInputStream(
                                        "eager attachment".toByteArray(
                                            Charsets.UTF_8
                                        )
                                    ),
                                    output
                                )
                            }
                            val eagerAttachment = dog.createAttachment(
                                eagerPlainTextFile,
                                "text/plain"
                            )
                            val streamAttachment = dog.createAttachment(
                                "text/plain",
                                "stream attachment".length.toLong(),
                                ByteArrayInputStream(
                                    "stream attachment".toByteArray(
                                        Charsets.UTF_8
                                    )
                                )
                            )
                            val imageAttachment = dog.createAttachment(
                                assetToFile("image.jpg")
                            )
                            val audioAttachment = dog.createAttachment(
                                assetToFile("clap.opus"),
                            )
                            assertEquals(
                                "8.853",
                                audioAttachment.attachment.metadataMap["duration"],
                                "audio attachment should include duration metadata"
                            )
                            assertEquals(
                                "application/x-lantern-waveform",
                                audioAttachment.thumbnail?.attachment?.mimeType,
                                "audio attachment should include waveform thumbnail"
                            )
                            val sentMsg = dog.sendToDirectContact(
                                catId,
                                "hello cat",
                                attachments = arrayOf(
                                    badAttachment,
                                    lazyAttachment,
                                    eagerAttachment,
                                    streamAttachment,
                                    imageAttachment,
                                    audioAttachment
                                )
                            )
                            val recvMsg = cat.waitFor<Model.StoredMessage>(
                                sentMsg.dbPath,
                                "cat should receive message"
                            )

                            assertEquals(
                                "hello cat",
                                recvMsg.text,
                                "cat should have received correct message text"
                            )
                            assertEquals(
                                5,
                                recvMsg.attachmentsCount,
                                "cat should have received 5 attachments"
                            )

                            suspend fun getAttachment(id: Int): Model.StoredAttachment? {
                                dog.waitFor<Model.StoredMessage>(
                                    recvMsg.dbPath,
                                    "waiting for attachment to download"
                                ) {
                                    it.attachmentsMap[id]?.status ==
                                        Model.StoredAttachment.Status.DONE
                                }.let { storedMsg ->
                                    return storedMsg.attachmentsMap[id]
                                }
                            }

                            fun text(attachment: Model.StoredAttachment?): String {
                                val out = ByteArrayOutputStream()
                                attachment?.inputStream?.use { Util.copy(it, out) }
                                return out.toString(Charsets.UTF_8.name())
                            }

                            val receivedLazyAttachment = getAttachment(1)
                            val receivedEagerAttachment = getAttachment(2)
                            val receivedStreamAttachment = getAttachment(3)
                            val receivedImageAttachment = getAttachment(4)
                            val receivedAudioAttachment = getAttachment(6)

                            assertEquals(
                                "lazy attachment",
                                text(receivedLazyAttachment)
                            )
                            assertEquals(
                                "eager attachment",
                                text(receivedEagerAttachment)
                            )
                            assertEquals(
                                "stream attachment",
                                text(receivedStreamAttachment)
                            )
                            assertEquals(
                                "image/jpeg",
                                receivedImageAttachment?.attachment?.mimeType
                            )
                            assertEquals(
                                "audio/opus",
                                receivedAudioAttachment?.attachment?.mimeType
                            )
                            assertNotNull(receivedImageAttachment?.thumbnail)
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testOrphanedAttachment() {
        testInCoroutine {
            newDB.use { dogDB ->
                newMessaging(dogDB, "dog").with { dog ->
                    val attachment = dog.createAttachment(
                        "text/plain",
                        5,
                        "hello".byteInputStream(Charsets.UTF_8)
                    )
                    assertTrue(
                        File(attachment.encryptedFilePath).exists(),
                        "attachment file should have been created"
                    )
                    dog.close()
                    // wait a couple of seconds
                    delay(2000)
                    newMessaging(dogDB, "dog").with {
                        assertFalse(
                            File(attachment.encryptedFilePath).exists(),
                            "orphaned attachment file should have been deleted"
                        )
                    }
                }
            }
        }
    }

    @Test
    fun testDeliveryStatus() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(catDB, "cat").with { cat ->
                        newMessaging(dogDB, "dog", stopSendRetryAfterMillis = 5000).with { dog ->
                            val catId = cat.store.identityKeyPair.publicKey.toString()
                            val dogId = dog.store.identityKeyPair.publicKey.toString()
                            val fakeId = KeyHelper.generateIdentityKeyPair().publicKey.toString()

                            cat.addOrUpdateDirectContact(dogId, "Dog")
                            dog.addOrUpdateDirectContact(catId, "Cat")
                            dog.addOrUpdateDirectContact(fakeId, "Fake")

                            val msg1 = dog.sendToDirectContact(catId, "hi cat")
                            dog.waitFor<Model.StoredMessage>(
                                msg1.dbPath,
                                "sending to real recipient should have succeeded"
                            ) {
                                it.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT
                            }

                            val msg2 = dog.sendToDirectContact(fakeId, "hi fake one")
                            dog.waitFor<Model.StoredMessage>(
                                msg2.dbPath,
                                "sending to fake recipient should have failed"
                            ) {
                                it.status == Model.StoredMessage.DeliveryStatus.COMPLETELY_FAILED
                            }

                            assertEquals(
                                0,
                                dog.db.list<Any>(Schema.PATH_OUTBOUND.path("%")).size,
                                "there should be no queued outbound messages once deliveries have succeeded and failed" // ktlint-disable max-line-length
                            )
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testReactions() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val catId = cat.myId.id
                            val dogId = dog.myId.id

                            dog.addOrUpdateDirectContact(catId, "Cat")
                            cat.addOrUpdateDirectContact(dogId, "Dog")

                            val msgs = sendAndVerify("dog sends to cat", dog, cat, "hi cat")
                            assertNotNull(msgs.received)

                            // add reaction from cat
                            cat.react(
                                msgs.received.dbPath,
                                smileyFace
                            ) // no, 'g' is not an emoticon, but it's just a test
                            var updatedMostRecentMsg =
                                cat.db.get<Model.StoredMessage>(msgs.received.dbPath)
                            assertNotNull(updatedMostRecentMsg)
                            assertEquals(
                                smileyFace,
                                updatedMostRecentMsg.getReactionsOrThrow(catId).emoticon,
                                "cat's reaction should have been recorded locally"
                            )

                            val dogMsg = dog.waitFor<Model.StoredMessage>(
                                msgs.sent.dbPath,
                                "dog should have gotten reaction"
                            ) {
                                it.reactionsCount > 0
                            }
                            assertEquals(
                                smileyFace,
                                dogMsg.getReactionsOrThrow(catId).emoticon,
                                "cat's reaction should have been recorded for dog too"
                            )

                            // clear reaction from cat
                            cat.react(
                                msgs.received.dbPath,
                                ""
                            )
                            updatedMostRecentMsg =
                                cat.db.get(msgs.received.dbPath)
                            assertNotNull(updatedMostRecentMsg)
                            assertEquals(
                                0,
                                updatedMostRecentMsg.reactionsCount,
                                "cat's reaction should have been cleared"
                            )

                            dog.waitFor<Model.StoredMessage>(
                                msgs.sent.dbPath,
                                "dog should have cleared reaction"
                            ) {
                                it.reactionsCount == 0
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testDeletions() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val catId = cat.myId.id
                            val dogId = dog.myId.id

                            dog.addOrUpdateDirectContact(catId, "Cat")
                            cat.addOrUpdateDirectContact(dogId, "Dog")

                            val initialMsgs = sendAndVerify(
                                "dog sends to cat", dog, cat, "hi cat",
                                attachments = arrayOf(
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
                            assertNotNull(initialMsgs.received)

                            // respond from cat
                            val replyMsgs = sendAndVerify(
                                "cat responds to dog",
                                cat,
                                dog,
                                "howdy",
                                replyToId = initialMsgs.sent.id,
                                attachments = arrayOf(
                                    dog.createAttachment(assetToFile("image.jpg"))
                                )
                            )

                            dog.react(
                                replyMsgs.received.dbPath,
                                smileyFace
                            )

                            // try to remotely delete message with someone other than sender
                            var bogusContactId = "blahblah".directContactId
                            try {
                                dog.deleteLocally(
                                    replyMsgs.received.dbPath,
                                    remotelyDeletedBy = bogusContactId
                                )
                                fail("deletion by anyone other than sender of message should not have been allowed") // ktlint-disable max-line-length
                            } catch (e: IllegalArgumentException) {
                                // okay
                            }

                            // globally delete cat's reply
                            cat.deleteGlobally(replyMsgs.sent.dbPath)
                            assertNull(
                                cat.db.get<Model.StoredMessage>(replyMsgs.sent.dbPath),
                                "message should have been deleted"
                            )
                            replyMsgs.sent.attachmentsMap.values.forEach { storedAttachment ->
                                assertFalse(
                                    File(storedAttachment.encryptedFilePath).exists(),
                                    "attachment file should have been deleted"
                                )
                            }
                            cat.db.get<Model.Contact>(replyMsgs.sent.contactId.contactPath)!!
                                .let { dogContact ->
                                    assertEquals(
                                        initialMsgs.received.ts,
                                        dogContact.mostRecentMessageTs,
                                        "mostRecentMessageTs for dog should have reverted to prior message" // ktlint-disable max-line-length
                                    )
                                    assertEquals(
                                        initialMsgs.received.text,
                                        dogContact.mostRecentMessageText,
                                        "mostRecentMessageText for dog should have reverted to prior message" // ktlint-disable max-line-length
                                    )
                                    assertEquals(
                                        initialMsgs.received.attachmentsMap.values.first().attachment.mimeType, // ktlint-disable max-line-length
                                        dogContact.mostRecentAttachmentMimeType,
                                        "mostRecentAttachmentMimeType for dog should be blank"
                                    )
                                }

                            val deletedMessage = dog.waitFor<Model.StoredMessage>(
                                replyMsgs.received.dbPath,
                                "message should have been marked deleted for dog too"
                            ) {
                                it.remotelyDeletedAt > 0
                            }
                            assertEquals(deletedMessage.contactId, deletedMessage.remotelyDeletedBy)
                            assertEquals("", deletedMessage.text)
                            assertEquals(0, deletedMessage.thumbnailsCount)
                            assertEquals(0, deletedMessage.attachmentsCount)
                            assertEquals(0, deletedMessage.reactionsCount)

                            replyMsgs.received.attachmentsMap.values.forEach { storedAttachment ->
                                assertFalse(
                                    File(storedAttachment.encryptedFilePath).exists(),
                                    "attachment file should have been deleted for dog too"
                                )
                            }

                            // dog locally deletes cat's message to clear remaining metadata
                            dog.deleteLocally(deletedMessage.dbPath)
                            dog.waitForNull(
                                replyMsgs.received.dbPath,
                                "message should have been completely deleted for dog"
                            )
                            dog.db.get<Model.Contact>(replyMsgs.received.contactId.contactPath)!!
                                .let { catContact ->
                                    assertEquals(
                                        initialMsgs.sent.ts,
                                        catContact.mostRecentMessageTs,
                                        "mostRecentMessageTs for cat should have reverted to prior message" // ktlint-disable max-line-length
                                    )
                                    assertEquals(
                                        initialMsgs.sent.text,
                                        catContact.mostRecentMessageText,
                                        "mostRecentMessageText for cat should have reverted to prior message" // ktlint-disable max-line-length
                                    )
                                    assertEquals(
                                        initialMsgs.sent.attachmentsMap.values.first().attachment.mimeType, // ktlint-disable max-line-length
                                        catContact.mostRecentAttachmentMimeType,
                                        "mostRecentAttachmentMimeType for cat should have reverted to prior message" // ktlint-disable max-line-length
                                    )
                                }

                            // locally delete dog's message
                            dog.deleteLocally(initialMsgs.sent.dbPath)
                            assertNull(
                                dog.db.get<Model.StoredMessage>(initialMsgs.sent.dbPath),
                                "message should have been deleted"
                            )
                            initialMsgs.sent.attachmentsMap.values.forEach { storedAttachment ->
                                assertFalse(
                                    File(storedAttachment.encryptedFilePath).exists(),
                                    "attachment file should have been deleted"
                                )
                            }
                            dog.db.get<Model.Contact>(initialMsgs.sent.contactId.contactPath)!!
                                .let { catContact ->
                                    assertEquals(
                                        0L,
                                        catContact.mostRecentMessageTs,
                                        "cat contact should have no most recent message timestamp"
                                    )
                                    assertEquals(
                                        "",
                                        catContact.mostRecentMessageText,
                                        "cat contact should have no most recent message text"
                                    )
                                    assertEquals(
                                        "",
                                        catContact.mostRecentAttachmentMimeType,
                                        "cat contact should have no most recent message attachment mime type" // ktlint-disable max-line-length
                                    )
                                }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testDisappearingMessages() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val catId = cat.myId.id
                            val dogId = dog.myId.id

                            val catContact = dog.addOrUpdateDirectContact(catId, "Cat")
                            val dogContact = cat.addOrUpdateDirectContact(dogId, "Dog")
                            assertEquals(
                                86400,
                                catContact.messagesDisappearAfterSeconds,
                                "messagesDisappearAfterSeconds should have defaulted to 1 day"
                            )
                            assertEquals(
                                0L,
                                catContact.firstReceivedMessageTs,
                                "contact should initially have no firstReceivedMessageTs"
                            )

                            // immediately hack the disappear settings to something different to make sure that the initial synchronization messages work
                            dog.db.mutate { tx ->
                                tx.get<Model.Contact>(catId.directContactPath)?.let {
                                    tx.put(
                                        it.dbPath,
                                        it.toBuilder().setMessagesDisappearAfterSeconds(17).build()
                                    )
                                }
                            }
                            cat.db.mutate { tx ->
                                tx.get<Model.Contact>(dogId.directContactPath)?.let {
                                    tx.put(
                                        it.dbPath,
                                        it.toBuilder().setMessagesDisappearAfterSeconds(17).build()
                                    )
                                }
                            }
                            val updatedCatContact = dog.waitFor<Model.Contact>(
                                catId.directContactPath,
                                "cat's initial disappear settings should arrive"
                            ) {
                                it.messagesDisappearAfterSeconds == 86400
                            }
                            assertNotEquals(0L, updatedCatContact.firstReceivedMessageTs)

                            cat.waitFor<Model.Contact>(
                                dogId.directContactPath,
                                "dog's initial disappear settings should arrive"
                            ) {
                                it.messagesDisappearAfterSeconds == 86400
                            }

                            // this is a very short value to allow us to test that messages don't disappear before they're sent
                            val disappearAfter = 1
                            dog.setDisappearSettings(catId.directContactPath, disappearAfter)
                            assertEquals(
                                disappearAfter,
                                dog.db.get<Model.Contact>(catContact.dbPath)
                                    ?.messagesDisappearAfterSeconds,
                                "messagesDisappearAfterSeconds should have changed locally"
                            )

                            cat.waitFor<Model.Contact>(
                                dogContact.dbPath,
                                "cat should have gotten updated messagesDisappearAfterSeconds for dog contact" // ktlint-disable max-line-length
                            ) {
                                it.messagesDisappearAfterSeconds == disappearAfter
                            }

                            val msgs = sendAndVerify(
                                "send disappearing message",
                                dog,
                                cat,
                                "hi cat",
                                ignoreSendsForMillis = 2000
                            )
                            dog.waitForNull(
                                msgs.sent.dbPath,
                                "message should have disappeared locally"
                            )
                            var remoteMsg = cat.db.get<Model.StoredMessage>(msgs.received.dbPath)
                            assertNotNull(
                                remoteMsg,
                                "message should not yet have disappeared remotely"
                            )

                            cat.markViewed(msgs.received.dbPath)
                            // close and reopen cat to make sure disappearing messages work after startup
                            cat.close()
                            newMessaging(catDB, "cat").with { theCat ->
                                remoteMsg = theCat.db.get(msgs.received.dbPath)
                                assertNotNull(
                                    remoteMsg,
                                    "message should still not have disappeared remotely after reopening messaging" // ktlint-disable max-line-length
                                )
                                assertTrue(
                                    remoteMsg!!.firstViewedAt > 0,
                                    "remoteMsg should be marked viewed"
                                )

                                theCat.waitForNull(
                                    msgs.received.dbPath,
                                    "message should have disappeared remotely"
                                )

                                assertTrue(
                                    dog.db.listPaths(Schema.PATH_DISAPPEARING_MESSAGES.path("%"))
                                        .isEmpty(),
                                    "disappearing message entry should be gone locally"
                                )
                                assertTrue(
                                    theCat.db.listPaths(Schema.PATH_DISAPPEARING_MESSAGES.path("%"))
                                        .isEmpty(),
                                    "disappearing message entry should be gone remotely"
                                )
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testIntroductions() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newDB.use { fishDB ->
                        newDB.use { ownerDB ->
                            newMessaging(dogDB, "dog").with { dog ->
                                newMessaging(catDB, "cat").with { cat ->
                                    newMessaging(fishDB, "fish").with { fish ->
                                        newMessaging(ownerDB, "owner").with { owner ->
                                            doTestIntroductions(dog, cat, fish, owner)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private suspend fun doTestIntroductions(
        dog: Messaging,
        cat: Messaging,
        fish: Messaging,
        owner: Messaging
    ) {
        val dogId = dog.myId.id
        val catId = cat.myId.id
        val fishId = fish.myId.id
        val ownerId = owner.myId.id

        // first connect owner with all the pets
        owner.addOrUpdateDirectContact(dogId, "Dog")
        dog.addOrUpdateDirectContact(ownerId, "Owner")
        owner.addOrUpdateDirectContact(catId, "Cat")
        cat.addOrUpdateDirectContact(ownerId, "Owner")
        owner.addOrUpdateDirectContact(fishId, "Fish")
        fish.addOrUpdateDirectContact(ownerId, "Owner")

        // call introduce with too few arguments
        try {
            owner.introduce(listOf(dogId))
            fail("Introducing only one contact should not work")
        } catch (e: java.lang.IllegalArgumentException) {
            // okay
        }

        // introduce all the pets to each other
        owner.introduce(listOf(dogId, catId, fishId))

        // introduce them again to make sure we can handle duplicate introductions
        owner.introduce(listOf(dogId, catId, fishId))

        // make sure everyone received an introduction to everyone
        val introducedParties = listOf(dog, cat, fish)
        introducedParties.forEach { me ->
            introducedParties.forEach { them ->
                if (me != them) {
                    val msgPath = me.waitFor<String>(
                        ownerId.introductionIndexPathByFrom(them.myId.id),
                        "${me.myId.id} should have gotten an introduction to ${them.myId.id}",
                        duration = 60.seconds,
                    )
                    val msg = me.db.get<Model.StoredMessage>(msgPath)
                    assertNotNull(msg)
                    assertTrue(msg.hasIntroduction())
                    assertEquals(
                        owner.introductionsDisappearAfterSeconds,
                        msg.disappearAfterSeconds
                    )
                }
            }
        }

        // test accepting introductions
        dog.acceptIntroduction(ownerId, catId)
        val catContact = dog.db.get<Model.Contact>(catId.directContactPath)
        assertNotNull(catContact, "dog should have a cat contact now")
        assertEquals(catId, catContact.contactId.id)
        assertEquals("Cat", catContact.displayName)
        assertEquals(
            Model.IntroductionDetails.IntroductionStatus.ACCEPTED,
            dog.db.introductionMessage(ownerId, catId)?.value?.introduction?.status
        )

        dog.acceptIntroduction(ownerId, fishId)
        cat.acceptIntroduction(ownerId, dogId)

        // send a duplicate introduction for fish from dog to cat (but with a different displayName)
        dog.addOrUpdateDirectContact(fishId, "Dogfish")
        dog.introduce(listOf(catId, fishId))
        cat.waitFor<String>(
            dogId.introductionIndexPathByFrom(fishId),
            "cat should have gotten an introduction to fish from dog"
        )
        cat.waitFor<String>(
            ownerId.introductionIndexPathByFrom(fishId),
            "cat should have gotten an introduction to fish from owner"
        )

        // accept the introduction and make sure status on both introduction messages is updated
        cat.acceptIntroduction(dogId, fishId)
        assertEquals(
            Model.IntroductionDetails.IntroductionStatus.ACCEPTED,
            cat.db.introductionMessage(dogId, fishId)?.value?.introduction?.status
        )
        var catToFishFromDog =
            cat.db.introductionMessage(ownerId, fishId)?.value
        assertEquals(
            Model.IntroductionDetails.IntroductionStatus.ACCEPTED,
            catToFishFromDog?.introduction?.status
        )
        assertEquals(
            "Fish",
            catToFishFromDog?.introduction?.originalDisplayName,
            "originalDisplayName on introduction should be retained"
        )
        assertEquals(
            "Dogfish",
            catToFishFromDog?.introduction?.displayName,
            "introduction should have updated to reflect accepted displayName"
        )
        val fishContact = cat.db.get<Model.Contact>(fishId.directContactPath)
        assertEquals(
            "Dogfish",
            fishContact?.displayName,
            "displayName on Contact should match version from accepted introduction"
        )

        // test deleting introduction messages
        fish.rejectIntroduction(ownerId, dogId)
        fish.rejectIntroduction(ownerId, catId)
        assertNull(
            fish.db.findOne(Schema.PATH_INTRODUCTIONS_BY_TO.path("%")),
            "all introductions by to index entries should be deleted"
        )
        assertNull(
            fish.db.findOne(Schema.PATH_INTRODUCTIONS_BY_FROM.path("%")),
            "all introductions by from index entries should be deleted"
        )

        try {
            fish.acceptIntroduction(ownerId, dogId)
            fail("accepting deleted introduction should not work")
        } catch (e: java.lang.IllegalArgumentException) {
            // okay
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
        ignoreSendsForMillis: Long = 0
    ): MessagePair {
        logger.debug("running case $testCase")
        val fromId = from.store.identityKeyPair.publicKey.toString()
        val toId = to.myId.id

        if (ignoreSendsForMillis > 0) {
            logger.debug("ignore sends for a while to make sure client handles this well")
            BrokenTransportFactory.ignoreOps.set(true)
            GlobalScope.launch {
                delay(ignoreSendsForMillis)
                logger.debug("start honoring sends again")
                BrokenTransportFactory.ignoreOps.set(false)
            }
        }

        // send a message
        var senderStoredMsg = from.sendToDirectContact(
            toId,
            text,
            attachments = attachments,
            replyToId = replyToId,
            replyToSenderId = replyToId?.let { toId }
        )
        assertFalse(senderStoredMsg.id.isNullOrBlank())
        assertEquals(Model.StoredMessage.DeliveryStatus.SENDING, senderStoredMsg.status, testCase)
        assertEquals(Model.MessageDirection.OUT, senderStoredMsg.direction, testCase)
        assertEquals(fromId, senderStoredMsg.senderId, testCase)
        assertTrue(senderStoredMsg.ts > 0, testCase)
        assertTrue(senderStoredMsg.ts < now, testCase)
        if (replyToId != null) {
            assertEquals(toId, senderStoredMsg.replyToSenderId)
            assertEquals(replyToId, senderStoredMsg.replyToId)
        }
        assertEquals(text, senderStoredMsg.text, testCase)

        if (ignoreSendsForMillis > 0) {
            // attempt to resend while message is still sending (should not be allowed)
            try {
                from.resendFailedMessage(senderStoredMsg.id)
                fail("Resending while message was still sending should have failed")
            } catch (e: java.lang.IllegalArgumentException) {
                assertEquals("Message is still in the process of sending", e.message)
            }
        }

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

        // wait for attachments to finish sending
        senderStoredMsg = from.waitFor(
            senderStoredMsg.dbPath,
            "wait for attachments to send"
        ) { storedMsg ->
            storedMsg.attachmentsMap.values.find {
                (it.status != Model.StoredAttachment.Status.DONE) ||
                    (it.hasThumbnail() && it.thumbnail.status != Model.StoredAttachment.Status.DONE)
            } == null
        }

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
                    File(storedAttachment.encryptedFilePath).length(),
                    AttachmentCipherOutputStream
                        .getCiphertextLength(storedAttachment.attachment.plaintextLength),
                    testCase
                )
            }
        }

        // ensure that recipient has received the message
        var recipientStoredMsg =
            to.waitFor<Model.StoredMessage>(senderStoredMsg.dbPath, testCase)
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
        assertNotEquals(0L, storedContact.firstReceivedMessageTs)
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
            // wait for all attachments and thumbnails to download
            recipientStoredMsg =
                to.waitFor(recipientStoredMsg.dbPath, testCase) { storedMsg ->
                    storedMsg.attachmentsMap.values.find {
                        (it.status != Model.StoredAttachment.Status.DONE) ||
                            (
                                it.hasThumbnail() &&
                                    it.thumbnail.status != Model.StoredAttachment.Status.DONE
                                )
                    } == null
                }
            recipientStoredMsg.attachmentsMap.forEach { (id, attachment) ->
                // make sure metadata matches expected
                assertEquals(
                    senderStoredMsg.getAttachmentsOrThrow(id).attachment.metadataMap,
                    attachment.attachment.metadataMap
                )
                // make sure decrypted content matches expected
                assertTrue(
                    Util.streamsEqual(
                        attachment.inputStream,
                        senderStoredMsg.getAttachmentsOrThrow(id).inputStream,
                    ),
                    testCase
                )
            }
        }

        return MessagePair(senderStoredMsg, recipientStoredMsg)
    }

    private fun newMessaging(
        db: DB,
        name: String,
        clientTimeoutMillis: Long = 5L.secondsToMillis,
        failedSendRetryDelayMillis: Long = 100,
        stopSendRetryAfterMillis: Long = 5L.minutesToMillis,
        orphanedAttachmentCutoffSeconds: Int = 1,
    ): Messaging {
        return Messaging(
            db,
            File(
                InstrumentationRegistry.getInstrumentation().targetContext.filesDir,
                "attachments"
            ),
            BrokenTransportFactory(
                "wss://tassis.lantern.io/api",
                (clientTimeoutMillis / 2)
            ),
            clientTimeoutMillis = clientTimeoutMillis,
            redialBackoffMillis = 50L,
            maxRedialDelayMillis = 500L,
            failedSendRetryDelayMillis = failedSendRetryDelayMillis,
            stopSendRetryAfterMillis = stopSendRetryAfterMillis,
            orphanedAttachmentCutoffSeconds = orphanedAttachmentCutoffSeconds,
            name = name
        )
    }
}

@ExperimentalTime
internal suspend fun <T : Any> Messaging.waitFor(
    path: String,
    testCase: String,
    duration: Duration = 10.seconds,
    check: ((T) -> Boolean)? = null
): T {
    val maxWait = duration.toLongMilliseconds()
    var elapsed = 0
    while (elapsed < maxWait) {
        val value = this.db.findOne<T>(path)
        if (value != null) {
            if (check == null || check(value)) {
                logger.debug("waited ${elapsed}ms to find match")
                return value
            }
        }
        delay(25)
        elapsed += 25
    }
    fail("waited ${elapsed}ms without finding match for '$testCase'")
}

@ExperimentalTime
internal suspend fun Messaging.waitForNull(
    path: String,
    testCase: String,
    duration: Duration = 10.seconds
) {
    val maxWait = duration.toLongMilliseconds()
    var elapsed = 0
    while (elapsed < maxWait) {
        val value = this.db.findOne<Any>(path)
        if (value == null) {
            logger.debug("waited ${elapsed}ms for value to be null")
            return
        }
        delay(25)
        elapsed += 25
    }
    throw AssertionError("waited ${elapsed}ms without value turning null for '$testCase'")
}

internal fun DB.dump() {
    val dumpString = dumpToString()
    println("DB Dump for ${this.get<Model.Contact>(Schema.PATH_ME)?.displayName}\n===============================================\n\n${dumpString}\n\n======================================") // ktlint-disable max-line-length
}

internal fun DB.dumpToString(): String =
    this.list<Any>("%").sortedBy { it.path }.joinToString("\n") {
        val valueString = when (val value = it.value) {
            is ByteArray -> value.base32
            else -> value.toString()
        }
        "${it.path}: $valueString"
    }

internal suspend fun Messaging.with(fn: suspend (messaging: Messaging) -> Unit) = this.use {
    try {
        fn(this)
    } catch (t: Throwable) {
        try {
            this.db.dump()
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
