package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.Transaction
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.TransportFactory
import io.lantern.messaging.tassis.byteString
import io.lantern.messaging.time.millisToNanos
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.secondsToMillis
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.TimeUnit

class UnknownSenderException : Exception("Unknown sender")

class Messaging(
    internal val store: MessagingStore,
    transportFactory: TransportFactory,
    clientTimeoutMillis: Long = 10L.secondsToMillis,
    redialBackoffMillis: Long = 500L,
    maxRedialDelayMillis: Long = 15L.secondsToMillis,
    failedSendRetryDelayMillis: Long = 5L.secondsToMillis,
    stopSendRetryAfterMillis: Long = 10L.minutesToMillis,
    numInitialPreKeysToRegister: Int = 5,
    internal val name: String = "messaging",
) : Closeable {
    internal val logger = KotlinLogging.logger(name)

    init {
        // Need to register types before starting crypto worker or doing anything else that accesses
        // database
        db.registerType(21, Model.Contact::class.java)
        db.registerType(22, Model.ShortMessageRecord::class.java)
        db.registerType(23, Model.OutgoingShortMessage::class.java)
    }

    internal val identityKeyPair = store.identityKeyPair
    internal val deviceId: DeviceId = store.deviceId

    val db: DB get() = store.db

    // All processing that involves crypto operations happens on this executor to keep the
    // SignalProtocolStore in a consistent state
    internal val cryptoWorker =
        CryptoWorker(this, failedSendRetryDelayMillis, stopSendRetryAfterMillis)
    internal val anonymousClientWorker =
        AnonymousClientWorker(
            transportFactory,
            this,
            clientTimeoutMillis,
            redialBackoffMillis,
            maxRedialDelayMillis
        )
    internal val authenticatedClientWorker =
        AuthenticatedClientWorker(
            transportFactory,
            this,
            clientTimeoutMillis,
            redialBackoffMillis,
            maxRedialDelayMillis
        )

    init {
        // make sure we have a contact entry for ourselves
        db.mutate { tx ->
            tx.get<Model.Contact>(Schema.PATH_ME) ?: tx.put(
                Schema.PATH_ME,
                Model.Contact.newBuilder().setId(identityKeyPair.publicKey.toString()).build()
            )
        }

        // on startup, register some pre keys
        // this also has the welcome side effect of starting an authenticated client, which we need
        // in order to receive messages
        cryptoWorker.registerPreKeys(numInitialPreKeysToRegister)
    }

    fun setMyDisplayName(displayName: String) {
        db.mutate { tx ->
            tx.put(
                Schema.PATH_ME,
                tx.get<Model.Contact>(Schema.PATH_ME)!!.toBuilder()
                    .setDisplayName(displayName).build()
            )
        }
    }

    // Adds or updates the given direct Contact
    fun addOrUpdateDirectContact(identityKey: String, displayName: String) {
        val path = identityKey.directContactPath
        db.mutate { tx ->
            val contactBuilder =
                tx.get<Model.Contact>(path)?.toBuilder() ?: Model.Contact.newBuilder()
                    .setType(Model.Contact.Type.DIRECT)
                    .setId(identityKey)
            val contact = contactBuilder.setDisplayName(displayName).build()
            tx.put(path, contact)
        }
    }

    // TODO: implement the below using a GroupCipher
//    fun sendToGroup(
//        groupId: String,
//        text: String?,
//        oggVoice: ByteArray? = null,
//    )

    /**
     * Send an outbound message from the user to a direct contact
     */
    fun sendToDirectContact(
        recipientId: String,
        text: String?,
        oggVoice: ByteArray? = null,
        replyToSenderId: String? = null,
        replyToId: String? = null
    ): Model.ShortMessageRecord {
        if (text.isNullOrBlank() && oggVoice == null) {
            throw IllegalArgumentException("Please specify either text or oggVoice")
        } else if (text != null && oggVoice != null) {
            throw IllegalArgumentException("Please specify either text or oggVoice but not both")
        } else if ((!replyToSenderId.isNullOrBlank() || !replyToId.isNullOrBlank()) && (replyToSenderId.isNullOrBlank() || replyToId.isNullOrBlank())) {
            throw IllegalArgumentException("If specifying either replyToSenderId and replyToId, please specify both")
        }
        val shortMessageBuilder = Model.ShortMessage.newBuilder().setId(randomMessageId)
        replyToSenderId?.let { shortMessageBuilder.setReplyToSenderId(it.fromBase32.byteString()) }
        replyToId?.let { shortMessageBuilder.setReplyToId(it.fromBase32.byteString()) }
        if (text != null) {
            shortMessageBuilder.text = text
        } else {
            shortMessageBuilder.oggVoice = oggVoice?.byteString()
        }
        val sent = nowUnixNano
        val msg = shortMessageBuilder.build()
        val out =
            Model.OutgoingShortMessage.newBuilder().setId(msg.id.base32)
                .setSent(sent)
                .setSenderId(store.identityKeyPair.publicKey.toString())
                .setRecipientId(recipientId)
        val msgRecordBuilder =
            Model.ShortMessageRecord.newBuilder()
                .setSenderId(store.identityKeyPair.publicKey.toString()).setId(msg.id.base32)
                .setTs(sent)
                .setMessage(msg.toByteString()).setDirection(Model.MessageDirection.OUT)
                .setStatus(Model.ShortMessageRecord.DeliveryStatus.SENDING)
        replyToSenderId?.let { msgRecordBuilder.setReplyToSenderId(it) }
        replyToId?.let { msgRecordBuilder.setReplyToId(it) }
        val msgRecord = msgRecordBuilder.build()
        db.mutate { tx ->
            // save the message in a list of all messages
            tx.put(msgRecord.dbPath, msgRecord)
            // update the relevant contact
            val contact = updateDirectContactMetaData(
                tx,
                recipientId,
                sent,
                Model.MessageDirection.OUT,
                msg.text
            )
            // save the message under the relevant contact messages
            tx.put(msgRecord.contactMessagePath(contact), msgRecord.dbPath)
            // enqueue the outgoing message in the db for sending (actual send happens in the
            // message processing loop)
            tx.put(msgRecord.outboundPath, out.build())
            cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
        }
        return msgRecord
    }

    internal fun updateDirectContactMetaData(
        tx: Transaction,
        identityKey: String,
        messageTs: Long = 0,
        messageDirection: Model.MessageDirection,
        messageText: String = ""
    ): Model.Contact {
        val contactPath = identityKey.directContactPath
        val contact = tx.get<Model.Contact>(contactPath)
            ?: throw IllegalArgumentException("unknown direct contact")
        if (messageTs <= contact.mostRecentMessageTs) {
            return contact
        }
        // delete existing index entry
        tx.delete(contact.timestampedIdxPath)
        // update the contact
        val updatedContact = contact.toBuilder().setMostRecentMessageTs(messageTs)
            .setMostRecentMessageDirection(messageDirection).setMostRecentMessageText(messageText)
            .build()
        tx.put(contactPath, updatedContact)
        // create a new index entry
        tx.put(updatedContact.timestampedIdxPath, contactPath)
        return updatedContact
    }

    // unregisters the current identity from tassis
    fun unregister() {
        authenticatedClientWorker.withClient { client ->
            client.unregister(object : Callback<Unit> {
                override fun onSuccess(result: Unit) {
                    logger.debug("successfully unregistered")
                }

                override fun onError(err: Throwable) {
                    logger.error("failed to unregister: ${err.message}", err)
                }
            })
        }
    }

    override fun close() {
        try {
            cryptoWorker.close()
            anonymousClientWorker.close()
            authenticatedClientWorker.close()
            cryptoWorker.executor.awaitTermination(10, TimeUnit.SECONDS)
            anonymousClientWorker.executor.awaitTermination(10, TimeUnit.SECONDS)
            authenticatedClientWorker.executor.awaitTermination(10, TimeUnit.SECONDS)
            store.close()
        } catch (t: Throwable) {
            logger.error(t.message)
        }
    }
}

val randomMessageId: ByteString
    get() {
        val uuid = UUID.randomUUID()
        val bytes = ByteArray(16)
        val bb = ByteBuffer.wrap(bytes)
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        return ByteString.copyFrom(bytes)
    }

val nowUnixNano: Long
    get() = System.currentTimeMillis().millisToNanos

