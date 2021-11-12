package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.db.DB
import io.lantern.messaging.conversions.byteString
import io.lantern.messaging.store.MessagingProtocolStore
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
import java9.util.concurrent.CompletableFuture
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
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.InvalidKeyException
import org.whispersystems.libsignal.util.InvalidCharacterException
import org.whispersystems.libsignal.util.KeyHelper
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import org.whispersystems.signalservice.internal.util.Util

private val logger = KotlinLogging.logger {}

private const val smileyFace = "\uD83D\uDE04"

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testSanitizeDisplayName() {
        assertEquals(
            "The Name 經被這些工業 5",
            " \n\tThe    \uD83D\uDE00\n\tName 經被這些工業 5\n\t  ".sanitizedDisplayName
        )
        assertTrue(" \n\t\uD83D\uDE00\n\t\uD83D\uDE02\n\t  ".sanitizedDisplayName.isEmpty())
    }

    @Test
    fun testManageDirectContact() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val dogId = dog.myId.id
                            val catId = cat.myId.id

                            try {
                                dog.addOrUpdateDirectContact(
                                    "blub",
                                    "Bogus"
                                )
                                fail("adding an invalid contact ID should fail")
                            } catch (e: InvalidKeyException) {
                                // okay
                            }

                            try {
                                dog.addOrUpdateDirectContact(
                                    "${catId}aaa",
                                    "Bogus"
                                )
                                fail("adding a too long contact ID should fail")
                            } catch (e: InvalidKeyException) {
                                // okay
                            }

                            try {
                                dog.addOrUpdateDirectContact("-${catId.substring(1, 52)}", "Bogus")
                                fail("adding a contact ID with an invalid character should fail")
                            } catch (e: InvalidCharacterException) {
                                // okay
                            }

                            val now = now
                            var catContact =
                                dog.addOrUpdateDirectContact(
                                    " ${catId.toUpperCase()} ",
                                    "\uD83D\uDE00   Cat\n",
                                    source = Model.ContactSource.APP1,
                                    applicationIds = mapOf(0 to "appid")
                                ) { appData ->
                                    appData["string"] = "string"
                                }
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
                            assertEquals(
                                Model.ContactSource.APP1,
                                catContact.source,
                                "cat should have correct source",
                            )
                            assertEquals(
                                mapOf(0 to "appid"),
                                catContact.applicationIdsMap,
                                "cat should have correct application IDs"
                            )
                            assertEquals(
                                mapOf(
                                    "string" to
                                        Model.Datum.newBuilder().setString("string").build()
                                ),
                                catContact.applicationDataMap,
                                "cat should have correct app data"
                            )
                            assertEquals(
                                Model.VerificationLevel.UNACCEPTED,
                                catContact.verificationLevel,
                                "verificationLevel should have defaulted to UNACCEPTED",
                            )
                            assertTrue(createdTs >= now, "createdTime should have been set")

                            val bytes = arrayOf<Byte>(5).toByteArray()
                            catContact = dog.addOrUpdateDirectContact(
                                catId, "New     Cat",
                                applicationIds = mapOf(1 to "otherappid")
                            ) { appData ->
                                appData["string"] = "string2"
                                appData["double"] = 1.0
                                appData["float"] = 2.0.toFloat()
                                appData["long"] = 3L
                                appData["int"] = 4
                                appData["bool"] = true
                                appData["bytes"] = bytes
                            }
                            assertEquals(
                                "New Cat",
                                catContact.displayName,
                                "displayName should have been changed"
                            )
                            assertEquals(
                                mapOf(0 to "appid", 1 to "otherappid"),
                                catContact.applicationIdsMap,
                                "cat should have full set of application IDs"
                            )
                            assertEquals(
                                mapOf(
                                    "string" to
                                        Model.Datum.newBuilder().setString("string2").build(),
                                    "double" to
                                        Model.Datum.newBuilder().setFloat(1.0).build(),
                                    "float" to
                                        Model.Datum.newBuilder().setFloat(2.0).build(),
                                    "long" to
                                        Model.Datum.newBuilder().setInt(3).build(),
                                    "int" to
                                        Model.Datum.newBuilder().setInt(4).build(),
                                    "bool" to
                                        Model.Datum.newBuilder().setBool(true).build(),
                                    "bytes" to
                                        Model.Datum.newBuilder().setBytes(
                                            bytes.byteString()
                                        ).build(),
                                ),
                                catContact.applicationDataMap,
                                "cat should have full app data"
                            )
                            assertEquals(
                                Model.VerificationLevel.UNACCEPTED,
                                catContact.verificationLevel,
                                "verificationLevel should have been left as UNACCEPTED",
                            )
                            assertEquals(
                                createdTs,
                                catContact.createdTs,
                                "createdTime should have been left alone"
                            )

                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
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

                            dog.waitFor<Model.Contact>(
                                catId.directContactPath,
                                "cat contact shows that a message has been received (in this case, the disappear settings message, since we haven't sent anything yet)" // ktlint-disable max-line-length
                            ) {
                                it.hasReceivedMessage
                            }

                            val msgs = sendAndVerify(
                                "cat sends another message to dog",
                                cat,
                                dog,
                                "hello again dog"
                            )

                            dog.deleteDirectContact(catId.toUpperCase())
                            assertFalse(dog.db.contains(catId.directContactId.contactPath))
                            assertFalse(dog.db.contains(msgs.received.dbPath))
                            assertEquals(
                                0,
                                dog.db.listPaths(Schema.PATH_CONTACT_MESSAGES.path("%")).count()
                            )
                            assertEquals(
                                1,
                                dog.db.listPaths(Schema.PATH_CONTACTS.path("%")).count(),
                                "dog now have a permanent contact"
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
                            dog.addOrUpdateDirectContact(
                                catId,
                                "New Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            cat.waitFor<Model.Contact>(
                                dogId.directContactPath,
                                "dog's initial disappear settings should arrive"
                            ) {
                                it.messagesDisappearAfterSeconds == 86400
                            }

                            sendAndVerify(
                                "cat sends a message to dog after having been removed and re-added", // ktlint-disable max-line-length
                                cat,
                                dog,
                                "hello again dog"
                            )

                            assertEquals(
                                2,
                                dog.db.listPaths(Schema.PATH_CONTACT_MESSAGES.path("%")).count(),
                                "dog should have 2 messages from cat, the message sent while cat was not a contact should have been included" // ktlint-disable max-line-length
                            )
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testChatNumber() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val catShortNumber = cat.waitFor<Model.Contact>(
                                Schema.PATH_ME,
                                "cat gets own ChatNumber)"
                            ) {
                                it.hasChatNumber()
                            }.chatNumber.shortNumber

                            val nonExistentNumber = CompletableFuture<Model.ChatNumber>()
                            dog.findChatNumberByShortNumber("asd") { number, err ->
                                if (err != null) {
                                    nonExistentNumber.completeExceptionally(err)
                                } else {
                                    nonExistentNumber.complete(number)
                                }
                            }
                            try {
                                nonExistentNumber.get()
                                fail("finding number using non-existent short number should fail")
                            } catch (t: Throwable) {
                                // okay
                            }

                            val catNumber = CompletableFuture<Model.ChatNumber>()
                            dog.findChatNumberByShortNumber(catShortNumber) { number, err ->
                                if (err != null) {
                                    catNumber.completeExceptionally(err)
                                } else {
                                    catNumber.complete(number)
                                }
                            }
                            assertEquals(catShortNumber, catNumber.get().shortNumber)

                            dog.addOrUpdateDirectContact(
                                chatNumber = catNumber.get(),
                                displayName = "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            sendAndVerify(
                                "dog sends a message to cat",
                                dog,
                                cat,
                                "hi cat"
                            )
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testDelayedChatNumber() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val catId = cat.myId.id

                            delay(5000)
                            logger.debug("before adding contact set dials to fail for a while")
                            BrokenTransportFactory.closeAll()
                            BrokenTransportFactory.succeedDialing.set(false)
                            val catContact = dog.addOrUpdateDirectContact(catId)
                            delay(5000)
                            dog.waitFor<Model.Contact>(
                                catContact.dbPath,
                                "cat contact should not yet have a chat number"
                            ) {
                                !it.hasChatNumber()
                            }
                            logger.debug("allow dials to succeed again")
                            BrokenTransportFactory.succeedDialing.set(true)
                            dog.waitFor<Model.Contact>(
                                catContact.dbPath,
                                "cat contact should now have a chat number"
                            ) {
                                it.hasChatNumber()
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testProvisionalContacts() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val dogId = dog.myId.id
                            val catId = cat.myId.id

                            logger.debug("Cat is $catId")
                            logger.debug("Dog is $dogId")

                            try {
                                dog.addProvisionalContact(
                                    // This is the length of a real public key, but has some
                                    // disallowed characters
                                    "bogus"
                                )
                                fail("adding an invalid contact ID should fail")
                            } catch (e: InvalidKeyException) {
                                // okay
                            }

                            assertEquals(
                                0,
                                dog.addProvisionalContact(
                                    catId,
                                    Model.ContactSource.APP1
                                ).mostRecentHelloTsMillis
                            )
                            assertEquals(
                                0,
                                cat.addProvisionalContact(
                                    dogId,
                                    verificationLevel = Model.VerificationLevel.UNVERIFIED
                                ).mostRecentHelloTsMillis
                            )

                            val catContact = dog.waitFor<Model.Contact>(
                                catId.directContactPath,
                                "dog should end up with verified cat contact"
                            ) {
                                it.mostRecentHelloTs > 0 &&
                                    it.verificationLevel == Model.VerificationLevel.VERIFIED
                            }
                            assertTrue(
                                catContact.displayName.isNullOrEmpty(),
                                "Cat contact should have no display name"
                            )
                            assertEquals(
                                Model.ContactSource.APP1,
                                catContact.source,
                                "Cat contact should have the right source"
                            )

                            val dogContact = cat.waitFor<Model.Contact>(
                                dogId.directContactPath,
                                "cat should end up with unverified dog contact"
                            ) {
                                it.mostRecentHelloTs > 0 &&
                                    it.verificationLevel ==
                                    Model.VerificationLevel.UNVERIFIED
                            }
                            assertTrue(
                                dogContact.displayName.isNullOrEmpty(),
                                "Dog contact should have no display name"
                            )

                            assertEquals(
                                0,
                                dog.db.listPaths(
                                    Schema.PATH_PROVISIONAL_CONTACTS.path("%")
                                ).size,
                                "dog should have no remaining provisional contacts"
                            )

                            assertEquals(
                                0,
                                cat.db.listPaths(
                                    Schema.PATH_PROVISIONAL_CONTACTS.path("%")
                                ).size,
                                "cat should have no remaining provisional contacts"
                            )

                            assertNotEquals(
                                0,
                                dog.addProvisionalContact(catId).mostRecentHelloTsMillis
                            )
                            assertFalse(
                                dog.db.contains(catId.provisionalContactPath),
                                "no provisional contact should be stored for an existing contact"
                            )

                            logger.debug("cat is deleting dog contact")
                            cat.deleteDirectContact(dogId)
                            logger.debug("cat is adding back dog provisional contact")
                            assertEquals(
                                0,
                                cat.addProvisionalContact(dogId).mostRecentHelloTsMillis
                            )
                            cat.waitFor<Model.Contact>(
                                dogId.directContactPath,
                                "cat should end up with dog contact again after having deleted dog"
                            )
                            dog.waitFor<Model.Contact>(
                                catId.directContactPath,
                                "mostRecentHelloTs on dog's cat contact should advance"
                            ) {
                                it.mostRecentHelloTs > catContact.mostRecentHelloTs
                            }

                            newDB.use { mouseDB ->
                                newMessaging(mouseDB, "mouse").with { mouse ->
                                    val mouseId = mouse.myId.id

                                    dog.addProvisionalContact(mouseId)
                                    assertEquals(
                                        1,
                                        dog.db.listPaths(
                                            Schema.PATH_PROVISIONAL_CONTACTS.path("%")
                                        ).size,
                                        "dog should have one provisional contact"
                                    )

                                    // wait for provisional contact to expire
                                    delay(21000)

                                    assertEquals(
                                        0,
                                        dog.db.listPaths(
                                            Schema.PATH_PROVISIONAL_CONTACTS.path("%")
                                        ).size,
                                        "dog should have no provisional contacts"
                                    )

                                    val dogDbString = dog.db.dumpToString()
                                    assertFalse(
                                        dogDbString.contains(mouseId),
                                        "dog's db should have no mention of mouse's ID"
                                    )
                                }
                            }

                            // make sure that we can successfully send messages even after the
                            // provisional contact deletion job has run
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
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testBlocking() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(dogDB, "dog").with { dog ->
                        newMessaging(catDB, "cat").with { cat ->
                            val dogId = dog.myId.id
                            val catId = cat.myId.id

                            dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            sendAndVerify(
                                "dog sends unsolicited message to cat",
                                dog,
                                cat,
                                "hi cat"
                            )

                            assertEquals(
                                1,
                                cat.db.listPaths(Schema.PATH_MESSAGES.path("%")).size,
                                "cat should have one message prior to blocking"
                            )

                            cat.blockDirectContact(dogId)
                            assertEquals(
                                0,
                                cat.db.listPaths(Schema.PATH_MESSAGES.path("%")).size,
                                "cat should have no messages after blocking"
                            )

                            dog.sendToDirectContact(catId, "hello again")
                            delay(10000)
                            assertEquals(
                                0,
                                cat.db.listPaths(Schema.PATH_MESSAGES.path("%")).size,
                                "cat should have no messages after blocking even though dog sent a message" // ktlint-disable max-line-length
                            )

                            cat.unblockDirectContact(dogId)
                            sendAndVerify(
                                "dog sends another message to cat after being unblocked",
                                dog,
                                cat,
                                "trust me now?" // ktlint-disable max-line-length
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
                val cat = newMessaging(catDB, "cat")
                val theDog = newMessaging(dogDB, "dog")

                testInCoroutine {
                    theDog.with { it ->
                        var dog = it
                        val catId = cat.identityKeyPair.publicKey.toString()
                        val dogId = dog.identityKeyPair.publicKey.toString()

                        assertNotNull(
                            dog.db.get<Model.Contact>(Schema.PATH_ME),
                            "self-contact should exist"
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
                        dog.addOrUpdateDirectContact(
                            catId,
                            "Cat",
                            minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                        )
                        // ensure that we immediately have a Contact
                        val storedContact = dog.db.get<Model.Contact>(catId.directContactPath)
                        assertTrue(storedContact != null)
                        assertEquals(catId, storedContact.contactId.id)
                        assertEquals("Cat", storedContact.displayName)

                        // now send a message from dog->cat before cat has come online
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

                        // start the Messaging system for cat, which will result in the registration\
                        // of pre keys, allowing the message to send successfully
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
                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )

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
                        val recoveryKey = generateRecoveryKey()
                        val catStore = MessagingProtocolStore(
                            catDB,
                            recoveryKey.keyPair("kp0")
                        )
                        val dogId = dog.myId.id
                        val catId = catStore.identityKeyPair.publicKey.toString()

                        dog.addOrUpdateDirectContact(
                            catId,
                            "Cat",
                            minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                        )
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
                            cat.recover(recoveryKey.base32)
                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )

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
                            dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
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
                            val catId = cat.identityKeyPair.publicKey.toString()
                            val dogId = dog.identityKeyPair.publicKey.toString()
                            val fakeId = KeyHelper.generateIdentityKeyPair().publicKey.toString()

                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            dog.addOrUpdateDirectContact(
                                fakeId,
                                "Fake",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )

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
    fun testMyself() {
        testInCoroutine {
            newDB.use { dogDB ->
                newMessaging(dogDB, "dog").with { dog ->
                    val msgs = sendAndVerify("dog sends note to dog", dog, dog, "hi myself")
                    assertNotNull(msgs.received)
                    val me = dog.db.get<Model.Contact>(Schema.PATH_ME)!!
                    assertEquals(true, me.isMe)
                    assertEquals(true, dog.db.get<Model.Contact>(me.dbPath)?.isMe)
                }
            }
        }
    }

    @Test
    fun testMyselfAttachments() {
        testInCoroutine {
            newDB.use { dogDB ->
                newMessaging(dogDB, "dog").with { dog ->
                    val dogId = dog.myId.id
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
                        "text/plain",
                        lazy = false
                    )
                    val sentMsg = dog.sendToDirectContact(
                        dogId,
                        "hello dog",
                        attachments = arrayOf(
                            lazyAttachment,
                            eagerAttachment,
                        )
                    )
                    val recvMsg = dog.waitFor<Model.StoredMessage>(
                        sentMsg.dbPath,
                        "dog should receive message with 2 completed attachments"
                    ) {
                        it.attachmentsMap.values.count {
                            it.status == Model.StoredAttachment.Status.DONE
                        } == 2
                    }

                    assertEquals(
                        2,
                        recvMsg.attachmentsCount,
                        "dog should have received 2 attachments"
                    )

                    fun getAttachment(id: Int): Model.StoredAttachment? {
                        return recvMsg.attachmentsMap[id]
                    }

                    fun text(attachment: Model.StoredAttachment?): String {
                        val out = ByteArrayOutputStream()
                        attachment?.inputStream?.use { Util.copy(it, out) }
                        return out.toString(Charsets.UTF_8.name())
                    }

                    val receivedLazyAttachment = getAttachment(0)
                    val receivedEagerAttachment = getAttachment(1)

                    assertEquals(
                        "lazy attachment",
                        text(receivedLazyAttachment)
                    )
                    assertEquals(
                        "eager attachment",
                        text(receivedEagerAttachment)
                    )
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

                            dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )

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

                            dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )

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

                            val catContact = dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            val dogContact = cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
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
                                    dog.db.listPaths(
                                        Schema.PATH_DISAPPEARING_MESSAGES.path("%")
                                    )
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
    fun testWebRTCSignaling() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newMessaging(catDB, "cat").with { cat ->
                        newMessaging(dogDB, "dog", stopSendRetryAfterMillis = 5000).with { dog ->
                            val catId = cat.identityKeyPair.publicKey.toString()
                            val dogId = dog.identityKeyPair.publicKey.toString()

                            cat.addOrUpdateDirectContact(
                                dogId,
                                "Dog",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )
                            dog.addOrUpdateDirectContact(
                                catId,
                                "Cat",
                                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
                            )

                            // give some time for pre keys to register
                            // TODO: would be nice to check for this more explicitly so we don't
                            // have a timing dependency
                            delay(5000)

                            val receivedFirstSignal = CompletableFuture<Void>()
                            val receivedSignals = HashMap<String, String>()
                            cat.subscribeToWebRTCSignals("subscriberId") { signal ->
                                receivedSignals.put(
                                    signal.senderId,
                                    signal.content.toString(Charsets.UTF_8)
                                )
                                receivedFirstSignal.complete(null)
                            }

                            var resultFuture = CompletableFuture<MultiDeviceResult>()
                            dog.sendWebRTCSignal(
                                catId,
                                "first".toByteArray(Charsets.UTF_8)
                            ) {
                                resultFuture.complete(it)
                            }
                            var result = resultFuture.get()
                            assertTrue(result.succeeded)
                            assertEquals(1, result.successfulDeviceIds?.size)

                            // wait for first signal, then unsubscribe
                            receivedFirstSignal.get()
                            cat.unsubscribeFromWebRTCSignals("subscriberId")

                            // now send another signal that shouldn't be recorded since we've
                            // unsubscribed
                            resultFuture = CompletableFuture<MultiDeviceResult>()
                            dog.sendWebRTCSignal(
                                catId,
                                "second".toByteArray(Charsets.UTF_8)
                            ) {
                                resultFuture.complete(it)
                            }
                            result = resultFuture.get()
                            assertTrue(result.succeeded)
                            assertEquals(1, result.successfulDeviceIds?.size)

                            // wait for cat to potentially receive 2nd signal (which it shouldn't)
                            delay(5000)

                            assertEquals(mapOf(dogId to "first"), receivedSignals)

                            // try to send to a non-existent device and make sure we get an error
                            val badDeviceId = DeviceId("nonexistent")
                            resultFuture = CompletableFuture<MultiDeviceResult>()
                            dog.sendWebRTCSignal(
                                catId,
                                "first".toByteArray(Charsets.UTF_8),
                                deviceId = badDeviceId.toString()
                            ) {
                                resultFuture.complete(it)
                            }
                            result = resultFuture.get()
                            assertFalse(
                                result.succeeded,
                                "sending signal to non-existent device should have failed"
                            )
                            assertEquals(
                                true,
                                result.deviceErrors?.containsKey(badDeviceId.toString())
                            )
                        }
                    }
                }
            }
        }
    }

    private fun testIntroductionsWith(
        fn: suspend (Messaging, Messaging, Messaging, Messaging) -> Unit
    ) {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { catDB ->
                    newDB.use { fishDB ->
                        newDB.use { ownerDB ->
                            newMessaging(dogDB, "dog").with { dog ->
                                newMessaging(catDB, "cat").with { cat ->
                                    newMessaging(fishDB, "fish").with { fish ->
                                        newMessaging(ownerDB, "owner").with { owner ->
                                            logger.debug("dogId: ${dog.myId.id}")
                                            logger.debug("catId: ${cat.myId.id}")
                                            logger.debug("fishId: ${fish.myId.id}")
                                            logger.debug("ownerId: ${owner.myId.id}")

                                            fn(dog, cat, fish, owner)
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

    @Test
    fun testIntroductions() {
        testIntroductionsWith { dog, cat, fish, owner ->
            val dogId = dog.myId.id
            val catId = cat.myId.id
            val fishId = fish.myId.id
            val ownerId = owner.myId.id

            // first connect owner with all the pets
            owner.addOrUpdateDirectContact(
                dogId,
                "Dog",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            dog.addOrUpdateDirectContact(
                ownerId,
                "Owner",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            dog.markDirectContactVerified(ownerId)
            owner.addOrUpdateDirectContact(
                catId,
                "Cat",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            cat.addOrUpdateDirectContact(
                ownerId,
                "Owner",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            owner.addOrUpdateDirectContact(
                fishId,
                "Fish",
                minimumVerificationLevel = Model.VerificationLevel.VERIFIED
            )
            fish.addOrUpdateDirectContact(
                ownerId,
                "Owner",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )

            // call introduce with too few arguments
            try {
                owner.introduce(listOf(dogId))
                fail("Introducing only one contact should not work")
            } catch (e: java.lang.IllegalArgumentException) {
                // okay
            }

            // Introduce dog and cat to each other. We use a custom introduction builder to mess up the
            // displayName on the introduction to test sanitizing inbound introductions.
            owner.doIntroduce(listOf(dogId, catId.toUpperCase())) { to ->
                Model.IntroductionDetails.newBuilder()
                    .setDisplayName("\uD83D\uDE00   ${to.displayName}  \n")
                    .build()
            }

            // introduce all pets again, including dog and cat, to make sure we can handle duplicate
            // introductions
            owner.introduce(listOf(dogId, catId, fishId.toUpperCase()))

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
                        val allIntroductionMessagesToThem = me.db
                            .listDetails<Model.StoredMessage>(
                                ownerId.directContactId.contactMessagesQuery
                            )
                            .filter {
                                it.value.hasIntroduction() && it.value.introduction.to == them.myId
                            }
                        assertEquals(
                            1,
                            allIntroductionMessagesToThem.size,
                            "should have only 1 introduction message from dog to them"
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
            assertEquals(Model.ContactSource.INTRODUCTION, catContact.source)
            assertEquals(
                Model.IntroductionDetails.IntroductionStatus.ACCEPTED,
                dog.db.introductionMessage(ownerId, catId)?.value?.value?.introduction?.status
            )

            // send a message to cat
            dog.sendToDirectContact(catId, text = "Hi Cat")
            val catContactAfterSend = dog.db.get<Model.Contact>(catId.directContactPath)
            assertFalse(
                catContactAfterSend?.hasReceivedMessage == true,
                "Should not have received message from cat prior to cat accepting introduction"
            )

            dog.acceptIntroduction(ownerId, fishId)
            assertEquals(
                Model.VerificationLevel.VERIFIED,
                dog.db.get<Model.Contact>(fishId.directContactPath)?.verificationLevel,
                "intro to verified contact from verified contact should look verified"
            )
            cat.acceptIntroduction(ownerId, dogId.toUpperCase())
            dog.waitFor<Model.Contact>(
                catId.directContactPath,
                "dog's cat contact should show that it received a message after cat accepted introduction" // ktlint-disable max-line-length
            ) {
                it.hasReceivedMessage
            }

            // send a duplicate introduction for fish from dog to cat (but with a different displayName)
            dog.addOrUpdateDirectContact(
                fishId,
                "Dogfish",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
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
                cat.db.introductionMessage(dogId, fishId)?.value?.value?.introduction?.status
            )
            var catToFishFromDog =
                cat.db.introductionMessage(ownerId, fishId)?.value?.value
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
            assertEquals(
                Model.VerificationLevel.UNVERIFIED,
                cat.db.get<Model.Contact>(fishId.directContactPath)?.verificationLevel,
                "intro to verified contact from unverified contact should look unverified"
            )

            // test deleting introduction messages
            fish.rejectIntroduction(ownerId, dogId)
            fish.rejectIntroduction(ownerId, catId.toUpperCase())
            fish.rejectIntroduction(dogId, catId)
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
    }

    @Test
    fun testIntroductionsBest() {
        testIntroductionsWith { dog, cat, fish, _ ->
            val dogId = dog.myId.id
            val catId = cat.myId.id
            val fishId = fish.myId.id

            // start with unsolicited introduction from dog
            dog.addOrUpdateDirectContact(
                catId,
                "Cat",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            dog.addOrUpdateDirectContact(
                fishId,
                "Fish",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            dog.markDirectContactVerified(fishId)
            fish.markDirectContactVerified(dogId)
            dog.introduce(listOf(catId, fishId))

            cat.waitFor<String>(
                dogId.introductionIndexPathByFrom(fishId),
                "Cat should have gotten an introduction to fish",
            )
            assertEquals(
                Model.VerificationLevel.UNACCEPTED,
                cat.db.listDetails<Model.StoredMessage>(
                    Schema.PATH_INTRODUCTIONS_BEST.path('%')
                ).firstOrNull()?.value?.introduction?.constrainedVerificationLevel,
                "cat's introduction should be unaccepted since dog is unaccepted"
            )

            fish.waitFor<String>(
                dogId.introductionIndexPathByFrom(catId),
                "Fish should have gotten an introduction to cat",
            )
            assertEquals(
                Model.VerificationLevel.UNVERIFIED,
                fish.db.listDetails<Model.StoredMessage>(
                    Schema.PATH_INTRODUCTIONS_BEST.path('%')
                ).firstOrNull()?.value?.introduction?.constrainedVerificationLevel,
                "fish's introduction should be unverified since introduction is unverified"
            )

            cat.markDirectContactVerified(dogId)
            assertEquals(
                Model.VerificationLevel.VERIFIED,
                cat.db.listDetails<Model.StoredMessage>(
                    Schema.PATH_INTRODUCTIONS_BEST.path('%')
                ).firstOrNull()?.value?.introduction?.constrainedVerificationLevel,
                "after verifying dog, cat's introduction should be verified"
            )
        }
    }

    @Test
    fun testIntroductionsAutoUpgrade() {
        testIntroductionsWith { dog, cat, fish, owner ->
            val dogId = dog.myId.id
            val catId = cat.myId.id
            val fishId = fish.myId.id
            val ownerId = owner.myId.id

            // add unverified dog contact
            fish.addOrUpdateDirectContact(
                dogId,
                "Dog",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            // add verified owner contact
            fish.addOrUpdateDirectContact(
                ownerId,
                "Owner",
                minimumVerificationLevel = Model.VerificationLevel.VERIFIED
            )

            // start with introduction to fish from dog
            dog.addOrUpdateDirectContact(
                catId,
                "Cat",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            dog.addOrUpdateDirectContact(
                fishId,
                "Fish",
                minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED
            )
            dog.introduce(listOf(catId, fishId))

            cat.waitFor<String>(
                dogId.introductionIndexPathByFrom(fishId),
                "Cat should have gotten an introduction to fish",
            )
            assertEquals(
                Model.VerificationLevel.UNACCEPTED,
                cat.db.listDetails<Model.StoredMessage>(
                    Schema.PATH_INTRODUCTIONS_BEST.path('%')
                ).firstOrNull()?.value?.introduction?.constrainedVerificationLevel,
                "cat's introduction should be unaccepted since dog is unaccepted"
            )

            fish.waitFor<String>(
                dogId.introductionIndexPathByFrom(catId),
                "Fish should have gotten an introduction to cat",
            )
            assertEquals(
                Model.VerificationLevel.UNVERIFIED,
                fish.db.listDetails<Model.StoredMessage>(
                    Schema.PATH_INTRODUCTIONS_BEST.path('%')
                ).firstOrNull()?.value?.introduction?.constrainedVerificationLevel,
                "fish's introduction should be unverified since introduction is unverified"
            )

            // Now have owner introduce fish and cat also. Unaccepted contact should be left alone,
            // unverified contact should auto-upgrade to verified
            fish.acceptIntroduction(dogId, catId)
            owner.addOrUpdateDirectContact(
                catId,
                "Cat",
                minimumVerificationLevel = Model.VerificationLevel.VERIFIED
            )
            owner.addOrUpdateDirectContact(
                fishId,
                "Fish",
                minimumVerificationLevel = Model.VerificationLevel.VERIFIED
            )
            cat.addOrUpdateDirectContact(
                ownerId,
                "Owner",
                minimumVerificationLevel = Model.VerificationLevel.VERIFIED
            )
            fish.addOrUpdateDirectContact(
                ownerId,
                "Owner",
                minimumVerificationLevel = Model.VerificationLevel.VERIFIED
            )
            owner.introduce(listOf(catId, fishId))

            val introductionToFishPath = cat.waitFor<String>(
                ownerId.introductionIndexPathByFrom(fishId),
                "Cat should have gotten an introduction from owner to fish",
            )
            assertEquals(
                Model.VerificationLevel.VERIFIED,
                cat.db.get<Model.StoredMessage>(introductionToFishPath)
                    ?.introduction?.constrainedVerificationLevel,
                "Cat's introduction from owner to fish should be verified"
            )
            assertEquals(
                Model.VerificationLevel.VERIFIED,
                cat.db.listDetails<Model.StoredMessage>(
                    Schema.PATH_INTRODUCTIONS_BEST.path('%')
                ).firstOrNull()?.value?.introduction?.constrainedVerificationLevel,
                "cat's best introduction should be verified since owner is verified"
            )
            cat.acceptIntroduction(ownerId, fishId)
            assertEquals(
                Model.VerificationLevel.VERIFIED,
                fish.db.get<Model.Contact>(catId.directContactPath)?.verificationLevel,
                "Fish should now be verified in cat's db"
            )

            val introductionToCatPath = fish.waitFor<String>(
                ownerId.introductionIndexPathByFrom(catId),
                "Fish should have gotten an introduction from owner to cat",
            )
            assertEquals(
                Model.IntroductionDetails.IntroductionStatus.ACCEPTED,
                fish.db.get<Model.StoredMessage>(introductionToCatPath)?.introduction?.status,
                "Fish's introduction from owner to cat should have been auto-accepted"
            )
            assertEquals(
                Model.VerificationLevel.VERIFIED,
                fish.db.get<Model.Contact>(catId.directContactPath)?.verificationLevel,
                "Cat should now be automatically verified in Fish's db"
            )
        }
    }

    @Test
    fun testSearch() {
        testInCoroutine {
            newDB.use { dogDB ->
                newMessaging(dogDB, "dog").with { dog ->
                    val dogId = dog.myId.id
                    val otherContactId = KeyHelper.generateIdentityKeyPair().publicKey.toString()

                    dog.addOrUpdateDirectContact(
                        otherContactId,
                        "The Dude",
                        minimumVerificationLevel = Model.VerificationLevel.UNVERIFIED,
                        applicationIds = mapOf(0 to "appid")
                    )
                    dog.sendToDirectContact(dogId, text = "Woof")

                    assertEquals(
                        0,
                        dog.searchContacts("Cat").size,
                        "search for non-existent contact should be empty"
                    )
                    assertEquals(
                        0,
                        dog.searchMessages("Meow").size,
                        "search for non-existent message should be empty"
                    )

                    val wildcard = otherContactId.substring(0, otherContactId.length - 2) + "*"
                    assertEquals(
                        otherContactId,
                        dog.searchContacts(wildcard).firstOrNull()?.value?.value?.contactId?.id,
                        "search for existing contact by id should yield that contact"
                    )
                    assertEquals(
                        otherContactId,
                        dog.searchContacts("appid").firstOrNull()?.value?.value?.contactId?.id,
                        "search for existing contact by application id should yield that contact"
                    )
                    assertEquals(
                        "The *Dud*e",
                        dog.searchContacts("dud*").firstOrNull()?.snippet,
                        "search for existing contact by display name should yield that contact"
                    )
                    assertEquals(
                        "*Woo*f",
                        dog.searchMessages("woo*").firstOrNull()?.snippet,
                        "search for existing message by text should yield that message"
                    )
                }
            }
        }
    }

    @Test
    fun testRecovery() {
        testInCoroutine {
            newDB.use { dogDB ->
                newDB.use { cat1DB ->
                    newDB.use { cat2DB ->
                        newMessaging(dogDB, "dog").with { dog ->
                            newMessaging(cat1DB, "cat").with { cat1 ->
                                newMessaging(cat2DB, "cat").with { cat2 ->
                                    val dogId = dog.myId.id
                                    val cat1Id = cat1.myId.id

                                    dog.addOrUpdateDirectContact(cat1Id)
                                    val dogContact = cat1.addOrUpdateDirectContact(dogId)
                                    sendAndVerify(
                                        "dog sends a message to cat 1",
                                        dog,
                                        cat1,
                                        "hi cat"
                                    )

                                    sendAndVerify(
                                        "cat1 sends a message to dog",
                                        cat1,
                                        dog,
                                        "hi dog"
                                    )

                                    cat1.recover(cat2.recoveryCode)
                                    assertNull(
                                        cat1.db.get<Model.Contact>(dogContact.dbPath),
                                        "dog contact should be gone after recovery"
                                    )

                                    cat1.addOrUpdateDirectContact(dogId)
                                    sendAndVerify(
                                        "recovered cat1 sends a message to dog",
                                        cat1,
                                        dog,
                                        "hello again dog"
                                    )
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Test
    fun testSha1() {
        assertEquals(173, "rtfr4noprty198jhdssetbegxq4y5fsh2rfrn96x7nx7tj8tutqy".sha1(360))
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
        val fromId = from.identityKeyPair.publicKey.toString()
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
            toId.toUpperCase(),
            text,
            attachments = attachments,
            replyToId = replyToId,
            unsafeReplyToSenderId = replyToId?.let { toId.toUpperCase() }
        )
        assertFalse(senderStoredMsg.id.isNullOrBlank())
        assertEquals(
            Model.StoredMessage.DeliveryStatus.SENDING,
            senderStoredMsg.status,
            testCase
        )
        if (fromId == toId) {
            assertEquals(Model.MessageDirection.IN, senderStoredMsg.direction, testCase)
            assertEquals(fromId, senderStoredMsg.senderId, testCase)
        } else {
            assertEquals(Model.MessageDirection.OUT, senderStoredMsg.direction, testCase)
            assertEquals(fromId, senderStoredMsg.senderId, testCase)
        }
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
        if (fromId == toId) {
            assertTrue(senderStoredMsg.ts <= recipientStoredMsg.ts, testCase)
        } else {
            assertTrue(senderStoredMsg.ts < recipientStoredMsg.ts, testCase)
        }
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
        provisionalContactsExpireAfterSeconds: Long = 20
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
            provisionalContactsExpireAfterSeconds = provisionalContactsExpireAfterSeconds,
            name = name
        )
    }
}

@ExperimentalTime
internal suspend fun <T : Any> Messaging.waitFor(
    path: String,
    testCase: String,
    duration: Duration = 20.seconds,
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
    println("DB Dump for ${this.get<Model.Contact>(Schema.PATH_ME)?.contactId?.id}\n===============================================\n\n${dumpString}\n\n======================================") // ktlint-disable max-line-length
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
