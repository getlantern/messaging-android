package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.Transaction
import io.lantern.messaging.metadata.Metadata
import io.lantern.messaging.store.MessagingProtocolStore
import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.TransportFactory
import io.lantern.messaging.tassis.byteString
import io.lantern.messaging.time.hoursToMillis
import io.lantern.messaging.time.secondsToMillis
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.signalservice.api.crypto.AttachmentCipherInputStream
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import org.whispersystems.signalservice.internal.util.Util
import java.io.ByteArrayInputStream
import java.io.Closeable
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlin.collections.HashSet

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

/**
 * Messaging provides an API for End to End Encrypted (E2EE) messaging, using a tassis server for
 * key distribution and message transport. Encryption is performed using a fork of Signal.
 *
 * @param parentDB the database in which Messaging will store its data. Signal ProtocolStore data
 *                 will go into the schema "messaging_protocol_store" and messaging data will go
 *                 into schema "messaging"
 * @param attachmentsDirectory the directory where encrypted attachments will be stored
 * @param transportFactory a source for Transports to connect to tassis
 * @param clientTimeoutMillis a timeout for dialing tassis and receiving an initial response
 * @param redialBackoffMillis if connecting to tassis fails, we back off exponentially and wait
 *                            redialBackoffMillis * 2 ^ numConsecutiveFailures before trying again
 * @param maxRedialDelayMillis caps how long to wait between redials to tassis
 * @param failedSendRetryDelayMillis how long to wait before retrying sends of failed messages
 * @param stopSendRetryAfterMillis if a message has been failing for this long, we stop trying to
 *                                 resend
 * @param numInitialPreKeysToRegister upon startup, Messaging will register this many preKeys with
 *                                    tassis
 * @param defaultMessagesDisappearAfterSeconds default disappearing message timer
 * @param orphanedAttachmentCutoffSeconds upon startup, any attachments in attachmentsDirectory that
 *                                        aren't in the database ("orphaned") and are older than
 *                                        orphanedAttachmentCutoffSeconds will be deleted from disk
 * @param name a name to use for this Messaging instance in logs
 * @param defaultConfiguration the default configuration to use prior to receiving a configuration
 *                             from tassis
 */
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
    private val orphanedAttachmentCutoffSeconds: Int = 86400, // 1 day
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
            cryptoWorker.submit { cryptoWorker.processOutbound(it.value.toBuilder()) }
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

        // on startup, go through all attachments older than orphanedAttachmentCutoffSeconds and
        // delete any that are not in the database
        deleteOrphanedAttachments()
    }

    /**
     * Updates the displayName associated with the "me" contact entry.
     */
    fun setMyDisplayName(displayName: String) {
        db.mutate { tx ->
            tx.put(
                Schema.PATH_ME,
                tx.get<Model.Contact>(Schema.PATH_ME)!!.toBuilder()
                    .setDisplayName(displayName).build()
            )
        }
    }

    /**
     * Adds or updates the given direct contact.
     *
     * @param id the base32 encoded public identity key of the contact
     * @param displayName the human-friendly display name for this contact
     */
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
                    contactBuilder.createdTs = now
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
     * Deletes the specified contact and all associated data.
     *
     * @param id the base32 encoded public identity key of the contact
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
     * Send an outbound message from the user to a direct contact.
     *
     * @param recipientId the base32 encoded public identity key of the recipient
     * @param text text for the message
     * @param replyToSenderId the id of the sender of the message to which we're replying
     * @param replyToId the id of the message to which we're replying (if replying to a message)
     * @param attachments any attachments to include with the message
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
        val sent = now
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
        attachments?.forEach { attachment ->
            msgBuilder.putAttachments(attachmentId, attachment)
            attachmentId++
            if (attachment.hasThumbnail()) {
                msgBuilder.putThumbnails(attachmentId, attachmentId - 1)
                attachmentId++
            }
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
            cryptoWorker.submit { cryptoWorker.processOutbound(out) }
            msg
        }
    }

    /**
     * Marks the message at the given path as viewed. This also starts the disappearing message
     * timer for the message based on the configured disappearingAfterSeconds for the conversation.
     */
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
            builder.firstViewedAt = now
            if (builder.disappearAfterSeconds > 0) {
                // set message to disappear now that it's been viewed
                builder.disappearAt = builder.firstViewedAt +
                    builder.disappearAfterSeconds.toLong().secondsToMillis
                tx.put(
                    builder.disappearingMessagePath,
                    msgPath
                )
            }
            tx.put(msgPath, builder.build())
        }
    }

    /**
     * Records a reaction to the message at the given msgPath, including sending the reaction to the
     * other parties in the conversation.
     *
     * @param msgPath path identifying the message to which we're reacting
     * @param emoticon 1 or 2 byte UTF emoticon for the reaction
     */
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
                        .setSent(now)
                        .setSenderId(myId.id)
                        .setRecipientId(msg.contactId.id) // TODO: this will need to change for groups
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutbound(out) }
            }
        }
    }

    /**
     * Updates the disappear settings for the conversation with the given contact, including
     * transmitting the updated settings to the other parties in the conversation.
     *
     * @param contactPath path to the Contact with whom we're conversing
     * @param disappearAfterSeconds messages in this conversation will automatically disappear after
     *                              this many seconds
     */
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
                .setSent(now)
                .setSenderId(myId.id)
                .setRecipientId(contactId) // TODO: this will need to change for groups
        tx.put(out.dbPath, out.build())
        cryptoWorker.submit { cryptoWorker.processOutbound(out) }
    }

    /**
     * Deletes the message locally and also notifies other participants in the conversation to
     * delete the message.
     *
     * @param msgPath the path identifying the message to delete.
     */
    fun deleteGlobally(msgPath: String) {
        db.mutate { tx ->
            deleteLocally(msgPath)?.let { msg ->
                val out =
                    Model.OutboundMessage.newBuilder()
                        .setDeleteMessageId(msg.id.fromBase32.byteString())
                        .setSent(now)
                        .setSenderId(myId.id)
                        .setRecipientId(msg.contactId.id) // TODO: this will need to change for groups
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutbound(out) }
            }
        }
    }

    /**
     * Deletes the message locally only.
     *
     * @param msgPath the path identifying the message to delete.
     * @param keepMetadata if true, we retain a record of the message and basic metadata, but delete
     *                     everything else
     */
    fun deleteLocally(msgPath: String, keepMetadata: Boolean = false): Model.StoredMessage? {
        return db.mutate { tx ->
            doDeleteLocally(tx, msgPath, keepMetadata)
        }
    }

    private fun doDeleteLocally(
        tx: Transaction,
        msgPath: String,
        keepMetadata: Boolean = false
    ): Model.StoredMessage? {
        return db.get<Model.StoredMessage>(msgPath)?.let { msg ->
            // Delete attachments on disk
            // Note - we only delete attachments locally, not in the cloud, because clients
            // aren't authorized to modify files. They will naturally be deleted once they hit
            // the server-side retention limit.
            msg.attachmentsMap.values.forEach { storedAttachment ->
                if (!File(storedAttachment.encryptedFilePath).delete()) {
                    logger.error("failed to delete attachment on disk, continuing")
                }
            }

            if (keepMetadata) {
                // don't actually physically delete the message yet, just clear the message content
                // and mark it as deleted
                tx.put(
                    msg.dbPath,
                    msg.toBuilder()
                        .setDeletedBySender(true)
                        .clearText()
                        .clearThumbnails()
                        .clearAttachments()
                        .clearReactions()
                        .build()
                )
            } else {
                // Delete the message
                tx.delete(msgPath)

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
            }
            return@let msg
        }
    }

    /**
     * Creates a StoredAttachment from the given File.
     *
     * @param file the File from which to create the attachment
     * @param mimeType the mime type to use if it cannot be auto-detected
     * @param metadata arbitrary metadata to associate with the attachment
     * @param lazy if true, we won't encrypt the attachment yet (will be encrypted upon sending)
     */
    fun createAttachment(
        file: File,
        mimeType: String? = null,
        metadata: Map<String, String>? = null,
        lazy: Boolean = true,
    ): Model.StoredAttachment {
        val md = try {
            Metadata.analyze(file, mimeType)
        } catch (t: Throwable) {
            logger.error("couldn't analyze metadata: ${t.message}")
            null
        }
        val attachment = Model.Attachment.newBuilder().setMimeType(md?.mimeType ?: mimeType)
        if (metadata != null) {
            attachment.putAllMetadata(metadata)
        }
        val storedAttachment =
            newStoredAttachment.setPlainTextFilePath(file.absolutePath)
                .setAttachment(attachment.build())
        if (md?.thumbnail != null) {
            storedAttachment.thumbnail = createAttachment(
                md.thumbnailMimeType,
                md.thumbnail.size.toLong(),
                ByteArrayInputStream(md.thumbnail)
            )
        }
        if (!lazy) {
            encryptAttachment(storedAttachment)
        }
        return storedAttachment.build()
    }

    /**
     * Creates a StoredAttachment from the given InputStream. The attachment is immediately
     * encrypted and stored to disk.
     *
     * @param mimeType the mime type to use if it cannot be auto-detected
     * @param length length of data in inputStream
     * @param inputStream the data to use for the attachment
     * @param metadata arbitrary metadata to associate with the attachment
     */
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

    /**
     * Encrypts the given attachment and stores it to disk.
     *
     * @param storedAttachment the attachment to encrypt
     * @param inputStream source of data to encrypt (if attachment wasn't created from a file)
     * @param length length of data in inputStream
     */
    fun encryptAttachment(
        storedAttachment: Model.StoredAttachment.Builder,
        inputStream: InputStream? = null,
        length: Long? = null
    ) {
        val plainTextFile = File(storedAttachment.plainTextFilePath)
        if (inputStream == null && !plainTextFile.exists()) {
            throw AttachmentPlainTextMissingException()
        }
        val attachmentBuilder = storedAttachment.attachment.toBuilder()
        encryptAttachment(
            attachmentBuilder,
            inputStream ?: FileInputStream(plainTextFile),
            FileOutputStream(storedAttachment.encryptedFilePath),
            length ?: plainTextFile.length()
        )
        storedAttachment.attachment = attachmentBuilder.build()
        storedAttachment.status = Model.StoredAttachment.Status.PENDING_UPLOAD
    }

    private fun encryptAttachment(
        attachmentBuilder: Model.Attachment.Builder,
        inputStream: InputStream,
        outputStream: OutputStream,
        length: Long
    ) {
        inputStream.use { input ->
            val maxLength = cfg.get().maxAttachmentSize
            if (AttachmentCipherOutputStream.getCiphertextLength(length) > maxLength) {
                val maxPlainTextLength =
                    maxLength - AttachmentCipherOutputStream.MAXIMUM_ENCRYPTION_OVERHEAD
                throw AttachmentTooBigException(maxPlainTextLength)
            }

            val keyMaterial = ByteArray(64)
            SecureRandom().nextBytes(keyMaterial)
            val output =
                AttachmentCipherOutputStream(
                    keyMaterial,
                    outputStream
                )
            val plaintextLength = Util.copy(input, output)
            attachmentBuilder.keyMaterial = keyMaterial.byteString()
            attachmentBuilder.plaintextLength = plaintextLength
            attachmentBuilder.digest = output.transmittedDigest.byteString()
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

    /**
     * Unregisters the current identity from tassis.
     */
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

    private fun deleteOrphanedAttachments() {
        // first build a set of all known attachment paths
        val knownAttachmentPaths = db
            .list<Model.StoredMessage>(Schema.PATH_MESSAGES.path("%"))
            .flatMap { msg ->
                msg.value.attachmentsMap.values.map { attachment -> attachment.encryptedFilePath }
            }
        // the walk the attachments directory tree and delete any old files with no known attachment
        // path
        deleteOrphanedAttachments(
            now,
            (orphanedAttachmentCutoffSeconds * 1000).toLong(),
            HashSet(knownAttachmentPaths),
            attachmentsDirectory
        )
    }

    private fun deleteOrphanedAttachments(
        now: Long,
        cutoffMillis: Long,
        knownAttachmentPaths: HashSet<String>,
        dir: File
    ) {
        if (dir.exists()) {
            dir.listFiles().let { files ->
                for (i in files.indices) {
                    files[i].let { file ->
                        if (file.isDirectory) {
                            deleteOrphanedAttachments(now, cutoffMillis, knownAttachmentPaths, file)
                        } else {
                            if (!knownAttachmentPaths.contains(file.absolutePath)) {
                                val fileOldEnough = now - file.lastModified() > cutoffMillis
                                if (fileOldEnough) {
                                    logger.debug(
                                        "deleting orphaned attachment ${file.absolutePath}"
                                    )
                                    file.delete()
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Closes this Messaging instance, including stopping all workers and disconnecting from tassis.
     */
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

val now: Long
    get() = System.currentTimeMillis()

val Model.StoredAttachment.inputStream: InputStream
    get() = AttachmentCipherInputStream.createForAttachment(
        File(encryptedFilePath),
        attachment.plaintextLength,
        attachment.keyMaterial.toByteArray(),
        attachment.digest.toByteArray()
    )
