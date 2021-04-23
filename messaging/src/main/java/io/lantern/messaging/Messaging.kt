package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.Transaction
import io.lantern.messaging.store.MessagingProtocolStore
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
import java.io.Closeable
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * This exception indicates that a message was received from an unknown sender (i.e. someone not in
 * the local contact list).
 */
class UnknownSenderException(internal val senderId: String, internal val messageId: String) :
    Exception("Unknown sender")

/**
 * This exception indicates that an attempt was made to upload an attachment larger than the
 * supported size.
 */
class AttachmentTooBigException(val maxAttachmentBytes: Long) : Exception("Attachment Too Big")

/**
 * This exception indicates that the file from which we attempted to create an attachment is missing
 */
class AttachmentPlainTextMissingException : Exception("Attachment Plaintext File Is Missing")

class Messaging(
    parentDB: DB,
    private val attachmentsDirectory: File,
    transportFactory: TransportFactory,
    clientTimeoutMillis: Long = 10L.secondsToMillis,
    redialBackoffMillis: Long = 500L,
    maxRedialDelayMillis: Long = 15L.secondsToMillis,
    failedSendRetryDelayMillis: Long = 5L.secondsToMillis,
    stopSendRetryAfterMillis: Long = 24L.hoursToMillis,
    numInitialPreKeysToRegister: Int = 5,
    private val defaultMessagesDisappearAfterSeconds: Int = 86400, // 1 day
    internal val name: String = "messaging",
    defaultConfiguration: Messages.Configuration = Messages.Configuration.newBuilder()
        .setMaxAttachmentSize(100000000).build()
) : Closeable {
    internal val logger = KotlinLogging.logger(name)
    internal val store = MessagingProtocolStore(parentDB)
    val db = parentDB.withSchema("messaging")

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
                db.get(Schema.PATH_CONFIG) ?: defaultConfiguration
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

    val myId: Model.ContactId

    init {
        // make sure we have a contact entry for ourselves
        val me = db.mutate { tx ->
            tx.get(Schema.PATH_ME) ?: tx.put(
                Schema.PATH_ME,
                Model.Contact.newBuilder()
                    .setContactId(identityKeyPair.publicKey.toString().directContactId).build()
            )
        }
        myId = me.contactId

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
                db.get<Model.StoredMessage>(inboundAttachment.msgPath)?.let { msg ->
                    cryptoWorker.downloadAttachment(
                        inboundAttachment,
                        msg.getAttachmentsOrThrow(inboundAttachment.attachmentId)
                    )
                }
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
    fun addOrUpdateDirectContact(id: String, displayName: String): Model.Contact =
        addOrUpdateContact(id.directContactId, displayName)

    private fun addOrUpdateContact(
        contactId: Model.ContactId,
        displayName: String
    ): Model.Contact {
        val path = contactId.contactPath
        return cryptoWorker.submitForValue {
            db.mutate { tx ->
                val existingContact = tx.get<Model.Contact>(path)
                val contactBuilder = existingContact?.toBuilder() ?: Model.Contact.newBuilder()
                    .setContactId(contactId)
                val isNew = existingContact == null
                if (isNew) {
                    contactBuilder.createdTs = nowUnixNano
                    contactBuilder.messagesDisappearAfterSeconds =
                        defaultMessagesDisappearAfterSeconds
                }
                val contact =
                    contactBuilder.setContactId(contactId).setDisplayName(displayName).build()
                tx.put(path, contact)
                // decrypt any "spam" we received from this Contact prior to adding them
                db.list<ByteArray>(contact.spamQuery)
                    .forEach { (spamPath, unidentifiedSenderMessage) ->
                        cryptoWorker.doDecryptAndStore(unidentifiedSenderMessage)
                        tx.delete(spamPath)
                    }
                if (isNew) {
                    // send our disappear settings as a kind of "hello", that also makes sure we're in sync on retention period
                    sendDisappearSettings(
                        tx,
                        contact.contactId.id,
                        contact.messagesDisappearAfterSeconds
                    )
                }
                contact
            }
        }
    }

    /**
     * Deletes the specified contact and all associated data
     */
    fun deleteDirectContact(id: String) {
        deleteContact(id.directContactId)
    }

    private fun deleteContact(contactId: Model.ContactId) {
        return cryptoWorker.submitForValue {
            db.mutate { tx ->
                tx.delete(contactId.contactPath)
                db.listPaths(contactId.contactByActivityQuery).forEach {
                    tx.delete(it)
                }
                tx.listPaths(contactId.spamQuery).forEach { path -> tx.delete(path) }
                tx.list<String>(contactId.contactMessagesQuery)
                    .forEach { doDeleteLocally(tx, it.value) }
                when (contactId.type) {
                    Model.ContactType.DIRECT -> {
                        store.deleteAllSessions(contactId.id)
                    }
                    else -> {
                        // TODO: support group contacts
                    }
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
    @Throws(IllegalArgumentException::class)
    fun sendToDirectContact(
        recipientId: String,
        text: String?,
        replyToSenderId: String? = null,
        replyToId: String? = null,
        attachments: Array<Model.StoredAttachment>? = null,
    ): Model.StoredMessage {
        if (text.isNullOrBlank() && attachments?.size == 0)
            throw IllegalArgumentException("Please specify either text or at least one attachment")
        else if ((!replyToSenderId.isNullOrBlank() || !replyToId.isNullOrBlank()) &&
            (replyToSenderId.isNullOrBlank() || replyToId.isNullOrBlank())
        ) {
            throw IllegalArgumentException(
                "If specifying either replyToSenderId and replyToId, please specify both"
            )
        }
        val recipient = db.get<Model.Contact>(recipientId.directContactPath)
            ?: throw IllegalArgumentException("Unknown recipient")

        val base32Id = randomMessageId.base32
        val sent = nowUnixNano
        val msgBuilder =
            Model.StoredMessage.newBuilder().setId(base32Id)
                .setContactId(recipientId.directContactId)
                .setSenderId(myId.id)
                .setTs(sent)
                .setText(text)
                .setDirection(Model.MessageDirection.OUT)
        replyToSenderId?.let { msgBuilder.setReplyToSenderId(it) }
        replyToId?.let { msgBuilder.setReplyToId(it) }
        if (recipient.messagesDisappearAfterSeconds > 0) {
            msgBuilder.disappearAfterSeconds = recipient.messagesDisappearAfterSeconds
        }
        val out =
            Model.OutboundMessage.newBuilder().setMessageId(base32Id)
                .setSent(sent)
                .setSenderId(myId.id)
                .setRecipientId(recipientId)
        var attachmentId = 0
        attachments?.forEach {
            msgBuilder.putAttachments(attachmentId, it)
            attachmentId++
        }
        replyToSenderId?.let { msgBuilder.setReplyToSenderId(it) }
        replyToId?.let { msgBuilder.setReplyToId(it) }
        val msg = msgBuilder.build()
        return db.mutate { tx ->
            // save the message in a list of all messages
            tx.put(msg.dbPath, msg)
            // update the relevant contact
            updateContactMetaData(tx, msg)
            // save the message under the relevant contact messages
            tx.put(msg.contactMessagePath, msg.dbPath)
            tx.put(out.dbPath, out.build())
            // immediately mark message viewed
            cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
            msg
        }
    }

    fun markViewed(msgPath: String) {
        db.mutate { tx ->
            tx.get<Model.StoredMessage>(msgPath)?.let { msg ->
                markViewed(tx, msg.toBuilder())
            }
        }
    }

    internal fun markViewed(tx: Transaction, builder: Model.StoredMessage.Builder) {
        val msgPath = builder.dbPath
        if (builder.firstViewedAt == 0L) {
            builder.firstViewedAt = nowUnixNano
            if (builder.disappearAfterSeconds > 0) {
                // set message to disappear now that it's been viewed
                builder.disappearAt = builder.firstViewedAt +
                    builder.disappearAfterSeconds.toLong().secondsToMillis.millisToNanos
                tx.put(
                    builder.disappearingMessagePath,
                    msgPath
                )
            }
            tx.put(msgPath, builder.build())
        }
    }

    fun react(msgPath: String, emoticon: String) {
        if (emoticon.length > 2) {
            throw IllegalArgumentException("emoticon must no more than 2 characters in length")
        }

        db.mutate { tx ->
            db.get<Model.StoredMessage>(msgPath)?.let { msg ->
                val reactingToSenderId = msg.senderId
                val reaction = Model.Reaction.newBuilder()
                    .setReactingToSenderId(reactingToSenderId.fromBase32.byteString())
                    .setReactingToMessageId(msg.id.fromBase32.byteString())
                    .setEmoticon(emoticon).build()
                // store our own reaction locally
                val builder = msg.toBuilder()
                // TODO: dry violation, this is repeated on receiving and sending ends
                if (reaction.emoticon == "") {
                    builder.removeReactions(myId.id)
                } else {
                    builder.putReactions(myId.id, reaction)
                }
                tx.put(msg.dbPath, builder.build())
                // send the reaction to other participants
                val out =
                    Model.OutboundMessage.newBuilder()
                        .setReaction(reaction.toByteString())
                        .setSent(nowUnixNano)
                        .setSenderId(myId.id)
                        .setRecipientId(msg.contactId.id) // TODO: this will need to change for groups
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
            }
        }
    }

    fun setDisappearSettings(contactPath: String, disappearAfterSeconds: Int) {
        // TODO: support group contacts
        db.mutate { tx ->
            db.get<Model.Contact>(contactPath)?.let { contact ->
                tx.put(
                    contactPath,
                    contact.toBuilder()
                        .setMessagesDisappearAfterSeconds(disappearAfterSeconds)
                        .build()
                )
                sendDisappearSettings(tx, contact.contactId.id, disappearAfterSeconds)
            }
        }
    }

    private fun sendDisappearSettings(
        tx: Transaction,
        contactId: String,
        disappearAfterSeconds: Int
    ) {
        val disappearSettings = Model.DisappearSettings.newBuilder()
            .setMessagesDisappearAfterSeconds(disappearAfterSeconds).build()
        val out =
            Model.OutboundMessage.newBuilder()
                .setDisappearSettings(disappearSettings.toByteString())
                .setSent(nowUnixNano)
                .setSenderId(myId.id)
                .setRecipientId(contactId) // TODO: this will need to change for groups
        tx.put(out.dbPath, out.build())
        cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
    }

    fun deleteGlobally(msgPath: String) {
        db.mutate { tx ->
            deleteLocally(msgPath)?.let { msg ->
                val out =
                    Model.OutboundMessage.newBuilder()
                        .setDeleteMessageId(msg.id.fromBase32.byteString())
                        .setSent(nowUnixNano)
                        .setSenderId(myId.id)
                        .setRecipientId(msg.contactId.id) // TODO: this will need to change for groups
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutgoing(out) }
            }
        }
    }

    fun deleteLocally(msgPath: String): Model.StoredMessage? {
        return db.mutate { tx ->
            doDeleteLocally(tx, msgPath)
        }
    }

    private fun doDeleteLocally(tx: Transaction, msgPath: String): Model.StoredMessage? {
        return db.get<Model.StoredMessage>(msgPath)?.let { msg ->
            tx.delete(msgPath)

            // Delete attachments on disk
            // Note - we only delete attachments locally, not in the cloud, because clients
            // aren't authorized to modify files. They will naturally be deleted once they hit
            // the server-side retention limit.
            msg.attachmentsMap.values.forEach { storedAttachment ->
                if (!File(storedAttachment.encryptedFilePath).delete()) {
                    logger.error("failed to delete attachment on disk, continuing")
                }
            }

            // Delete index entries for messages under this Contact's conversation
            tx.delete(msg.contactMessagePath)

            // Update the Contact metadata based on the most recent remaining message
            tx.listDetails<Model.StoredMessage>(
                msg.contactMessagesQuery,
                count = 1,
                reverseSort = true
            ).let { storedMessages ->
                val mostRecentMsg = storedMessages.firstOrNull()
                if (mostRecentMsg != null) {
                    updateContactMetaData(tx, mostRecentMsg.value, force = true)
                } else {
                    clearContactMetaData(tx, msg.contactId)
                }
            }

            return@let msg
        }
    }

    // Creates a StoredAttachment from the given File
    fun createAttachment(
        file: File,
        mimeType: String? = null,
        metadata: Map<String, String>? = null,
        lazy: Boolean = true,
    ): Model.StoredAttachment {
        val attachment = Model.Attachment.newBuilder().setMimeType(mimeType)
        if (metadata != null) {
            attachment.putAllMetadata(metadata)
        }
        val storedAttachment =
            newStoredAttachment.setPlainTextFilePath(file.absolutePath)
                .setAttachment(attachment.build())
        if (!lazy) {
            encryptAttachment(storedAttachment)
        }
        return storedAttachment.build()
    }

    // Creates a StoredAttachment from the given InputStream
    internal fun createAttachment(
        mimeType: String? = null,
        length: Long,
        inputStream: InputStream,
        metadata: Map<String, String>? = null
    ): Model.StoredAttachment {
        val attachment = Model.Attachment.newBuilder().setMimeType(mimeType)
        if (metadata != null) {
            attachment.putAllMetadata(metadata)
        }
        val storedAttachment =
            newStoredAttachment.setAttachment(attachment.build())
        encryptAttachment(storedAttachment, inputStream, length)
        return storedAttachment.build()
    }

    fun encryptAttachment(
        storedAttachment: Model.StoredAttachment.Builder,
        inputStream: InputStream? = null,
        length: Long? = null
    ) {
        val plainTextFile = File(storedAttachment.plainTextFilePath)
        if (inputStream == null && !plainTextFile.exists()) {
            throw AttachmentPlainTextMissingException()
        }
        (inputStream ?: FileInputStream(plainTextFile)).use { input ->
            val maxLength = cfg.get().maxAttachmentSize
            if (AttachmentCipherOutputStream.getCiphertextLength(
                    length ?: plainTextFile.length()
                ) > maxLength
            ) {
                val maxPlainTextLength =
                    maxLength - AttachmentCipherOutputStream.MAXIMUM_ENCRYPTION_OVERHEAD
                throw AttachmentTooBigException(maxPlainTextLength)
            }

            val keyMaterial = ByteArray(64)
            SecureRandom().nextBytes(keyMaterial)
            val output =
                AttachmentCipherOutputStream(
                    keyMaterial,
                    FileOutputStream(storedAttachment.encryptedFilePath)
                )
            val plaintextLength = Util.copy(input, output)
            val attachmentBuilder =
                storedAttachment.attachment.toBuilder()
                    .setKeyMaterial(keyMaterial.byteString())
                    .setPlaintextLength(plaintextLength)
                    .setDigest(output.transmittedDigest.byteString())
            storedAttachment.attachment = attachmentBuilder.build()
            storedAttachment.status = Model.StoredAttachment.Status.PENDING_UPLOAD
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
                ).joinToString(File.separator)
            )
            if (!subDirectory.exists() && !subDirectory.mkdirs()) {
                throw RuntimeException("Unable to make attachments sub-directory $subDirectory")
            }
            return Model.StoredAttachment.newBuilder().setGuid(guid)
                .setEncryptedFilePath(File(subDirectory, guid).absolutePath)
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
                msg.attachmentsMap.values.iterator().next().attachment.mimeType
        }
        val updatedContact = updatedContactBuilder.build()
        tx.put(contactPath, updatedContact)
        // create a new index entry
        tx.put(updatedContact.timestampedIdxPath, contactPath)
    }

    private fun clearContactMetaData(
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
        } catch (t: Throwable) {
            logger.error(t.message)
        }
    }
}

private val randomMessageId: ByteString
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
        File(encryptedFilePath),
        attachment.plaintextLength,
        attachment.keyMaterial.toByteArray(),
        attachment.digest.toByteArray()
    )
