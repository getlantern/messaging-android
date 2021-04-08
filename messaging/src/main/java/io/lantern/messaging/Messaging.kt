package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.Transaction
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.TransportFactory
import io.lantern.messaging.tassis.byteString
import io.lantern.messaging.time.hoursToMillis
import io.lantern.messaging.time.millisToNanos
import io.lantern.messaging.time.secondsToMillis
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.signalservice.api.crypto.AttachmentCipherInputStream
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import org.whispersystems.signalservice.internal.util.Util
import java.io.*
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class UnknownSenderException(internal val senderId: String, internal val messageId: String) :
    Exception("Unknown sender")

class AttachmentTooBigException(val maxAttachmentBytes: Long) : Exception("Attachment Too Big")

class Messaging(
    private val attachmentsDirectory: File,
    internal val store: MessagingStore,
    transportFactory: TransportFactory,
    clientTimeoutMillis: Long = 10L.secondsToMillis,
    redialBackoffMillis: Long = 500L,
    maxRedialDelayMillis: Long = 15L.secondsToMillis,
    failedSendRetryDelayMillis: Long = 5L.secondsToMillis,
    stopSendRetryAfterMillis: Long = 24L.hoursToMillis,
    numInitialPreKeysToRegister: Int = 5,
    internal val name: String = "messaging",
    defaultConfiguration: Messages.Configuration = Messages.Configuration.newBuilder()
        .setMaxAttachmentSize(100000000).build()
) : Closeable {
    internal val logger = KotlinLogging.logger(name)

    val db: DB get() = store.db

    init {
        // register protocol buffer types before starting crypto worker or doing anything else that
        // accesses the database
        db.registerType(20, Messages.Configuration::class.java)
        db.registerType(21, Model.Contact::class.java)
        db.registerType(22, Model.StoredMessage::class.java)
        db.registerType(23, Model.OutboundMessage::class.java)
        db.registerType(24, Model.InboundAttachment::class.java)
    }

    private val cfg = AtomicReference<Messages.Configuration>()

    init {
        // initialize configuration
        db.mutate { tx ->
            val latestCfg =
                db.get<Messages.Configuration>(Schema.PATH_CONFIG) ?: defaultConfiguration
            cfg.set(latestCfg)
            tx.put(Schema.PATH_CONFIG, latestCfg)
        }
    }

    internal val identityKeyPair = store.identityKeyPair
    internal val deviceId: DeviceId = store.deviceId

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
                Model.Contact.newBuilder().setContactId(
                    Model.ContactId.newBuilder().setType(Model.ContactType.DIRECT)
                        .setId(identityKeyPair.publicKey.toString()).build()
                ).build()
            )
        }

        // immediately request some upload authorizations so that we're ready to upload attachments
        cryptoWorker.submit { cryptoWorker.getMoreUploadAuthorizationsIfNecessary() }

        // on startup, read all pending OutboundMessages to try reprocessing them
        db.list<Model.OutboundMessage>(Schema.PATH_OUTBOUND.path("%")).forEach {
            cryptoWorker.submit { cryptoWorker.processOutgoing(it.value.toBuilder()) }
        }

        // on startup, read all pending InboundAttachments to try downloading them
        db.list<Model.InboundAttachment>(Schema.PATH_INBOUND_ATTACHMENTS.path("%")).forEach {
            cryptoWorker.submit {
                val inboundAttachment = it.value
                val storedMsg = db.get<Model.StoredMessage>(inboundAttachment.msgPath)
                cryptoWorker.downloadAttachment(
                    inboundAttachment,
                    storedMsg!!.getAttachmentsOrThrow(inboundAttachment.attachmentId)
                )
            }
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
    fun addOrUpdateContact(type: Model.ContactType, id: String, displayName: String) {
        val contactId = Model.ContactId.newBuilder()
            .setType(type)
            .setId(id).build()
        val path = contactId.contactPath
        cryptoWorker.submitForValue {
            db.mutate { tx ->
                val contactBuilder =
                    tx.get<Model.Contact>(path)?.toBuilder() ?: Model.Contact.newBuilder()
                        .setContactId(contactId)
                val contact =
                    contactBuilder.setContactId(contactId).setDisplayName(displayName).build()
                tx.put(path, contact)
                // decrypt any "spam" we received from this Contact prior to adding them
                db.list<ByteArray>(contact.spamQuery)
                    .forEach { (spamPath, unidentifiedSenderMessage) ->
                        cryptoWorker.doDecryptAndStore(unidentifiedSenderMessage)
                        tx.delete(spamPath)
                    }
            }
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
        replyToSenderId: String? = null,
        replyToId: String? = null,
        attachments: Array<Model.StoredAttachment>? = null,
    ): Model.StoredMessage {
        if (text.isNullOrBlank() && attachments?.size == 0) {
            throw IllegalArgumentException("Please specify either text or at least one attachment")
        } else if ((!replyToSenderId.isNullOrBlank() || !replyToId.isNullOrBlank()) && (replyToSenderId.isNullOrBlank() || replyToId.isNullOrBlank())) {
            throw IllegalArgumentException("If specifying either replyToSenderId and replyToId, please specify both")
        }
        val base32Id = randomMessageId.base32
        val sent = nowUnixNano
        val senderId = store.identityKeyPair.publicKey.toString()
        val msgBuilder =
            Model.StoredMessage.newBuilder().setId(base32Id)
                .setContactId(
                    Model.ContactId.newBuilder().setType(Model.ContactType.DIRECT)
                        .setId(recipientId).build()
                )
                .setSenderId(senderId)
                .setTs(sent)
                .setText(text)
                .setDirection(Model.MessageDirection.OUT)
        replyToSenderId?.let { msgBuilder.setReplyToSenderId(it) }
        replyToId?.let { msgBuilder.setReplyToId(it) }
        val out =
            Model.OutboundMessage.newBuilder().setMessageId(base32Id)
                .setSent(sent)
                .setSenderId(senderId)
                .setRecipientId(recipientId)
        var attachmentId = 0
        attachments?.forEach {
            msgBuilder.putAttachments(attachmentId, it)
            attachmentId++
        }
        replyToSenderId?.let { msgBuilder.setReplyToSenderId(it) }
        replyToId?.let { msgBuilder.setReplyToId(it) }
        val msg = msgBuilder.build()
        db.mutate { tx ->
            // save the message in a list of all messages
            tx.put(msg.dbPath, msg)
            // update the relevant contact
            updateContactMetaData(tx, msg)
            // save the message under the relevant contact messages
            tx.put(msg.contactMessagePath, msg.dbPath)
            tx.put(out.dbPath, out.build())
            cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
        }
        return msg
    }

    fun deleteGlobally(msgPath: String) {
        db.mutate { tx ->
            deleteLocally(msgPath)?.let { storedMsg ->
                val senderId = store.identityKeyPair.publicKey.toString()
                val out =
                    Model.OutboundMessage.newBuilder()
                        .setDeleteMessageId(storedMsg.id.fromBase32.byteString())
                        .setSent(nowUnixNano)
                        .setSenderId(senderId)
                        .setRecipientId(storedMsg.contactId.id) // TODO: this will need to change for groups
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
            }
        }
    }

    fun react(msgPath: String, emoticon: String) {
        if (emoticon.length > 1) {
            throw IllegalArgumentException("emoticon must be 0 or 1 characters in length")
        }

        db.mutate { tx ->
            db.get<Model.StoredMessage>(msgPath)?.let { storedMsg ->
                val reactingToSenderId = storedMsg.senderId
                val senderId = store.identityKeyPair.publicKey.toString()
                if (senderId == reactingToSenderId) {
                    throw IllegalArgumentException("can't react to your own message")
                }

                val reaction = Model.Reaction.newBuilder()
                    .setReactingToSenderId(reactingToSenderId.fromBase32.byteString())
                    .setReactingToMessageId(storedMsg.id.fromBase32.byteString())
                    .setEmoticon(emoticon).build()
                // store our own reaction locally
                val builder = storedMsg.toBuilder()
                // TODO: dry violation, this is repeated on receiving and sending ends
                if (reaction.emoticon == "") {
                    builder.removeReactions(senderId)
                } else {
                    builder.putReactions(senderId, reaction)
                }
                tx.put(storedMsg.dbPath, builder.build())
                // send the reaction to other participants
                val out =
                    Model.OutboundMessage.newBuilder()
                        .setReaction(reaction.toByteString())
                        .setSent(nowUnixNano)
                        .setSenderId(senderId)
                        .setRecipientId(storedMsg.contactId.id) // TODO: this will need to change for groups
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
            }
        }
    }

    fun deleteLocally(msgPath: String): Model.StoredMessage? {
        return db.mutate { tx ->
            db.get<Model.StoredMessage>(msgPath)?.let { storedMsg ->
                tx.delete(msgPath)

                // Delete attachments on disk
                // Note - we only delete attachments locally, not in the cloud, because clients
                // aren't authorized to modify files. They will naturally be deleted once they hit
                // the server-side retention limit.
                storedMsg.attachmentsMap.values.forEach { storedAttachment ->
                    if (!File(storedAttachment.filePath).delete()) {
                        logger.error("failed to delete attachment on disk, continuing")
                    }
                }

                // Delete index entries for messages under this Contact's conversation
                tx.delete(storedMsg.contactMessagePath)

                // Update the Contact metadata based on the most recent remaining message
                tx.listDetails<Model.StoredMessage>(
                    storedMsg.contactMessagesQuery,
                    count = 1,
                    reverseSort = true
                ).let { storedMessages ->
                    val mostRecentMsg = storedMessages.firstOrNull()
                    if (mostRecentMsg != null) {
                        updateContactMetaData(tx, mostRecentMsg.value, force = true)
                    } else {
                        clearContactMetaData(tx, storedMsg.contactId)
                    }
                }

                return@let storedMsg
            }
        }
    }

    // Creates a StoredAttachment from the given File
    fun createAttachment(
        mimeType: String,
        file: File,
        metadata: Map<String, String>? = null,
    ): Model.StoredAttachment =
        createAttachment(mimeType, file.length(), FileInputStream(file), metadata)

    /**
     * Creates a StoredAttachment from the data read from the given InputStream.
     */
    fun createAttachment(
        mimeType: String,
        length: Long,
        input: InputStream,
        metadata: Map<String, String>? = null,
    ): Model.StoredAttachment {
        input.use {
            val maxLength = cfg.get().maxAttachmentSize
            if (AttachmentCipherOutputStream.getCiphertextLength(length) > maxLength) {
                throw AttachmentTooBigException(maxLength - AttachmentCipherOutputStream.MAXIMUM_ENCRYPTION_OVERHEAD)
            }

            val storedAttachment = newStoredAttachment
            val keyMaterial = ByteArray(64)
            SecureRandom().nextBytes(keyMaterial)
            val output =
                AttachmentCipherOutputStream(
                    keyMaterial,
                    FileOutputStream(storedAttachment.filePath)
                )
            val plaintextLength = Util.copy(input, output)
            val attachmentBuilder = Model.Attachment.newBuilder().setMimeType(mimeType)
                .setKeyMaterial(keyMaterial.byteString())
                .setPlaintextLength(plaintextLength)
                .setDigest(output.transmittedDigest.byteString())
            metadata?.let { attachmentBuilder.putAllMetadata(it) }
            return storedAttachment.setAttachment(attachmentBuilder.build()).build()
        }
    }

    internal val newStoredAttachment: Model.StoredAttachment.Builder
        get() {
            val guid = UUID.randomUUID().toString()
            // break attachmentsDirectory into smaller subfolders to avoid having too many files in any one folder
            val subDirectory = File(
                arrayOf(
                    attachmentsDirectory.absolutePath,
                    guid.substring(0, 1),
                    guid.substring(1, 2),
                    guid.substring(2, 3)
                ).joinToString(File.pathSeparator)
            )
            if (!subDirectory.mkdirs()) {
                throw RuntimeException("Unable to make attachments sub-directory ${subDirectory}")
            }
            return Model.StoredAttachment.newBuilder().setGuid(guid)
                .setFilePath(File(subDirectory, guid).absolutePath)
        }

    internal fun updateContactMetaData(
        tx: Transaction,
        msg: Model.StoredMessage,
        force: Boolean = false,
    ) {
        val contactPath = msg.contactId.contactPath
        val contact = tx.get<Model.Contact>(contactPath)
            ?: throw IllegalArgumentException("unknown contact")
        if (!force && msg.ts <= contact.mostRecentMessageTs) {
            return
        }
        // delete existing index entry
        tx.delete(contact.timestampedIdxPath)
        // update the contact
        val updatedContactBuilder = contact.toBuilder().setMostRecentMessageTs(msg.ts)
            .setMostRecentMessageDirection(msg.direction).setMostRecentMessageText(msg.text)
        if (msg.attachmentsCount > 0) {
            updatedContactBuilder.mostRecentAttachmentMimeType =
                msg.attachmentsMap[0]!!.attachment.mimeType
        }
        val updatedContact = updatedContactBuilder.build()
        tx.put(contactPath, updatedContact)
        // create a new index entry
        tx.put(updatedContact.timestampedIdxPath, contactPath)
    }

    internal fun clearContactMetaData(
        tx: Transaction,
        contactId: Model.ContactId
    ) {
        val contactPath = contactId.contactPath
        tx.get<Model.Contact>(contactPath)?.let { contact ->
            // delete existing index entry
            tx.delete(contact.timestampedIdxPath)
            // update the contact
            val updatedContact =
                contact.toBuilder()
                    .clearMostRecentMessageTs()
                    .clearMostRecentMessageDirection()
                    .clearMostRecentMessageText()
                    .clearMostRecentAttachmentMimeType().build()
            tx.put(contactPath, updatedContact)
        }
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

    internal fun updateConfig(newCfg: Messages.Configuration) {
        db.mutate { tx ->
            tx.put(Schema.PATH_CONFIG, newCfg)
            this.cfg.set(newCfg)
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

val Model.StoredAttachment.inputStream: InputStream
    get() = AttachmentCipherInputStream.createForAttachment(
        File(filePath),
        attachment.plaintextLength,
        attachment.keyMaterial.toByteArray(),
        attachment.digest.toByteArray()
    )