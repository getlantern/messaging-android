package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.SearchResult
import io.lantern.db.SnippetConfig
import io.lantern.db.Transaction
import io.lantern.messaging.conversions.byteString
import io.lantern.messaging.metadata.Metadata
import io.lantern.messaging.store.MessagingProtocolStore
import io.lantern.messaging.tassis.Callback
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.TassisError
import io.lantern.messaging.tassis.TransportFactory
import io.lantern.messaging.time.hoursToMillis
import io.lantern.messaging.time.secondsToMillis
import java.io.ByteArrayInputStream
import java.io.Closeable
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.math.BigInteger
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.Locale
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.InvalidKeyException
import org.whispersystems.libsignal.ecc.ECKeyPair
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.fingerprint.NumericFingerprintGenerator
import org.whispersystems.libsignal.util.InvalidCharacterException
import org.whispersystems.signalservice.api.crypto.AttachmentCipherInputStream
import org.whispersystems.signalservice.api.crypto.AttachmentCipherOutputStream
import org.whispersystems.signalservice.internal.util.Util

/**
 * This exception indicates that a provisional message like a hello was received from an unknown
 * sender (i.e. someone not in the list of provisional contacts)
 */
class UnknownProvisionalSenderException : Exception("Unknown provisional sender")

/**
 * This exception indicates that a message was received from a blocked sender.
 */
class BlockedSenderException(internal val senderId: String) :
    Exception("Blocked sender")

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
 * The result of adding a provisional contact.
 */
data class ProvisionalContactResult(
    // The timestamp of the most recent hello we received from this contact prior to adding a
    // provisional contact. A value greater than 0 means that we already have a Contact entry.
    val mostRecentHelloTsMillis: Long,
    // The timestamp in milliseconds since epoch when the provisional contact expires. A value of 0
    // means that  we didn't add a provisional contact and don't have to worry about it expiring.
    val expiresAtMillis: Long
)

private val emptyBytes = ByteArray(0)

// we use 5200 fingerprint iterations just like Signal does
private const val fingerprintIterations = 5200

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
 * @param keyPair if specified, initializes the messaging protocol store with this private key
 */
class Messaging(
    private val parentDB: DB,
    private val attachmentsDirectory: File,
    private val transportFactory: TransportFactory,
    private val clientTimeoutMillis: Long = 10L.secondsToMillis,
    private val redialBackoffMillis: Long = 500L,
    private val maxRedialDelayMillis: Long = 15L.secondsToMillis,
    private val failedSendRetryDelayMillis: Long = 5L.secondsToMillis,
    private val stopSendRetryAfterMillis: Long =
        1000 * 365 * 24L.hoursToMillis, // approximately 1000 years
    private val numInitialPreKeysToRegister: Int = 5,
    private val defaultMessagesDisappearAfterSeconds: Int = 86400, // 1 day
    private val orphanedAttachmentCutoffSeconds: Int = 86400, // 1 day
    internal val introductionsDisappearAfterSeconds: Int = 86400 * 7, // 7 days
    internal val provisionalContactsExpireAfterSeconds: Long = 300, // 5 minutes
    internal val name: String = "messaging",
    private val defaultConfiguration: Messages.Configuration = Messages.Configuration.newBuilder()
        .setMaxAttachmentSize(100000000).build(),
) : Closeable {
    internal val logger = KotlinLogging.logger(name)
    val db = parentDB.withSchema("messaging")
    val store = MessagingProtocolStore(parentDB, identityKeyPair)

    private val webRTCSignalingSubscribers = ConcurrentHashMap<String, (WebRTCSignal) -> Unit>()

    private val cfg = AtomicReference<Messages.Configuration>()

    internal val identityKeyPair: ECKeyPair
        get() = recoveryKey.keyPair("kp0")

    internal val deviceId: DeviceId
        get() = store.deviceId

    // All processing that involves crypto operations happens on this executor to keep the
    // SignalProtocolStore in a consistent state
    private val cryptoWorkerRef = AtomicReference<CryptoWorker>()

    internal val cryptoWorker: CryptoWorker
        get() = cryptoWorkerRef.get()

    private val myIdRef = AtomicReference<Model.ContactId>()

    val myId: Model.ContactId
        get() = myIdRef.get()

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
        // register protocol buffer types before starting crypto worker or doing anything else that
        // accesses the database
        db.registerType(20, Messages.Configuration::class.java)
        db.registerType(21, Model.Contact::class.java)
        db.registerType(22, Model.StoredMessage::class.java)
        db.registerType(23, Model.OutboundMessage::class.java)
        db.registerType(24, Model.InboundAttachment::class.java)
        db.registerType(25, Model.ProvisionalContact::class.java)

        // apply migrations
        Migrations(this).apply()

        initialize()
    }

    private fun startCryptoWorker() {
        cryptoWorkerRef.set(
            CryptoWorker(
                this,
                failedSendRetryDelayMillis,
                stopSendRetryAfterMillis
            )
        )
    }

    private fun initialize() {
        // initialize configuration
        db.mutate { tx ->
            val latestCfg =
                db.get(Schema.PATH_CONFIG) ?: defaultConfiguration
            cfg.set(latestCfg)
            tx.put(Schema.PATH_CONFIG, latestCfg)
        }

        startCryptoWorker()

        // make sure we have a contact entry for ourselves
        val me = db.mutate { tx ->
            tx.get(Schema.PATH_ME) ?: tx.put(
                Schema.PATH_ME,
                Model.Contact.newBuilder()
                    .setContactId(identityKeyPair.publicKey.toString().directContactId)
                    .setIsMe(true)
                    .build()
            )
        }
        myIdRef.set(me.contactId)

        // add myself as a contact to record notes to myself
        doAddOrUpdateContact(myId) { contact, _ ->
            contact.isMe = true
        }

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

        // on startup, request ChatNumbers for any direct contacts that don't yet have them
        db.listPaths(
            Schema.PATH_CONTACTS.path(Schema.CONTACT_DIRECT_PREFIX)
        ).forEach { path ->
            lookupChatNumberIfNecessary(path)
        }
    }

    private val recoveryKey: RecoveryKey
        get() = db.mutate { tx ->
            tx.get(Schema.PATH_RECOVERY_KEY) ?: run {
                val newRecoveryKey = generateRecoveryKey()
                tx.put(Schema.PATH_RECOVERY_KEY, newRecoveryKey)
                newRecoveryKey
            }
        }

    /**
     * Returns the recovery code used by this Messaging instance encoded in base32.
     *
     * TODO: use constant-time implementation of base32 encoding
     */
    val recoveryCode = recoveryKey.base32

    /**
     * Recovers the messaging system using the given recoveryKey. All existing data will be erased.
     */
    fun recover(recoveryCode: String) {
        cryptoWorker.close()
        authenticatedClientWorker.disconnect()
        db.clear()
        // TODO: use constant-time implementation of base32 decoding
        val rk = recoveryCode.fromBase32
        db.mutate { tx ->
            tx.put(Schema.PATH_RECOVERY_KEY, rk)
        }
        store.changeIdentityKeyPair(identityKeyPair)
        initialize()
    }

    /**
     * Adds or updates the given direct contact. The contact is identified by either unsafeId or
     * chatNumber.
     *
     * @param unsafeId the base32 encoded public identity key of the contact
     *                 (if unspecified, chatNumber is used instead)
     * @param displayName optional human-friendly display name for this contact
     *                    (if unspecified, existing displayName is left alone)
     * @param source optional identifier of the source of this contact
     *               (if unspecified, existing source is left alone)
     * @param applicationIds optional map of application-specific ids to associate with the user.
     *                       ID is limited to int32.
     *                       (application IDs are added to existing set)
     * @param minimumVerificationLevel the contact's verification will be set to the greater of this
     *                                 or the current verificationLevel (defaults to UNVERIFIED)
     * @param chatNumber the ChatNumber to associate with this contact
     *                   (if unspecified, the existing ChatNumber is left alone)
     * @param updateApplicationData optional function to manipulate application specific data on the
     *                              contact
     * @return the created or updated Contact
     */
    @Throws(
        InvalidKeyException::class,
        InvalidCharacterException::class,
        java.lang.IllegalArgumentException::class,
    )
    fun addOrUpdateDirectContact(
        unsafeId: String? = null,
        displayName: String? = null,
        source: Model.ContactSource? = null,
        applicationIds: Map<Int, String>? = null,
        minimumVerificationLevel: Model.VerificationLevel =
            Model.VerificationLevel.UNACCEPTED,
        chatNumber: Model.ChatNumber? = null,
        updateApplicationData: ((MutableMap<String, Any>) -> Unit)? = null
    ): Model.Contact {
        if (unsafeId == null && chatNumber == null) {
            throw java.lang.IllegalArgumentException("Please specify either unsafeId or chatNumber")
        }

        val contactId = when {
            unsafeId != null -> unsafeId.directContactId
            chatNumber != null -> chatNumber.directContactId
            else -> throw java.lang.IllegalArgumentException("Please specify either unsafeId or chatNumber") // ktlint-disable max-line-length
        }

        return cryptoWorker.submitForValue {
            addOrUpdateContact(
                contactId,
                displayName,
                source,
                applicationIds,
                minimumVerificationLevel,
                chatNumber,
                updateApplicationData
            )
        }
    }

    @Throws(InvalidKeyException::class)
    internal fun addOrUpdateContact(
        unsafeContactId: Model.ContactId,
        displayName: String? = null,
        source: Model.ContactSource? = null,
        applicationIds: Map<Int, String>? = null,
        minimumVerificationLevel: Model.VerificationLevel,
        chatNumber: Model.ChatNumber? = null,
        updateApplicationData: ((MutableMap<String, Any>) -> Unit)? = null
    ): Model.Contact =
        doAddOrUpdateContact(unsafeContactId) { contact, _ ->
            chatNumber?.let {
                if (chatNumber.isComplete) {
                    // only record complete ChatNumbers
                    // if we don't have a complete ChatNumber, we'll later get it from the server
                    contact.chatNumber = chatNumber
                }
            }
            displayName?.let { contact.displayName = it }
            source?.let { it -> contact.source = it }
            applicationIds?.let { it ->
                contact.putAllApplicationIds(it)
            }
            contact.verificationLevelValue =
                minimumVerificationLevel.number.coerceAtLeast(contact.verificationLevelValue)
            updateApplicationData?.let { update ->
                val appData = mutableMapOf<String, Any>()
                contact.applicationDataMap.forEach { (key, value) ->
                    appData[key] = when (value.valueCase) {
                        Model.Datum.ValueCase.STRING -> value.string
                        Model.Datum.ValueCase.FLOAT -> value.float
                        Model.Datum.ValueCase.INT -> value.int
                        Model.Datum.ValueCase.BOOL -> value.bool
                        Model.Datum.ValueCase.BYTES -> value.bytes.toByteArray()
                        Model.Datum.ValueCase.VALUE_NOT_SET ->
                            throw Error("Value on Datum should always be set")
                    }
                }
                contact.clearApplicationData()
                update(appData)
                val updatedAppData = mutableMapOf<String, Model.Datum>()
                appData.forEach { (key, value) ->
                    val datum = Model.Datum.newBuilder()
                    when (value) {
                        is String -> datum.string = value
                        is Double -> datum.float = value
                        is Float -> datum.float = value.toDouble()
                        is Long -> datum.int = value
                        is Int -> datum.int = value.toLong()
                        is Boolean -> datum.bool = value
                        is ByteArray -> datum.bytes = value.byteString()
                        else -> throw RuntimeException(
                            "Unrecognized value type ${value.javaClass.name}"
                        )
                    }
                    updatedAppData[key] = datum.build()
                }
                contact.putAllApplicationData(updatedAppData)
            }
        }

    /**
     * Accepts a previously unaccepted Contact, putting it into UNVERIFIED status.
     */
    fun acceptDirectContact(unsafeId: String) =
        doAddOrUpdateContact(unsafeId.directContactId) { contact, _ ->
            contact.verificationLevel = Model.VerificationLevel.UNVERIFIED
        }

    /**
     * Marks a contact as VERIFIED.
     */
    fun markDirectContactVerified(unsafeId: String) =
        doAddOrUpdateContact(unsafeId.directContactId) { contact, _ ->
            contact.verificationLevel = Model.VerificationLevel.VERIFIED
        }

    /**
     * Blocks a direct contact, resulting in all of their past and future communication going into a
     * spam folder.
     */
    fun blockDirectContact(unsafeId: String) =
        doAddOrUpdateContact(unsafeId.directContactId) { contact, _ ->
            contact.blocked = true
        }

    /**
     * Unblocks a direct contact, allowing us to receive messages from this contact again.
     */
    fun unblockDirectContact(unsafeId: String) =
        doAddOrUpdateContact(unsafeId.directContactId) { contact, _ ->
            contact.blocked = false
        }

    /**
     * Adds or updates the contact identified by the given unsafeContactId, using the provided
     * update function to set properties of the Contact.
     */
    @Throws(InvalidKeyException::class)
    internal fun doAddOrUpdateContact(
        unsafeContactId: Model.ContactId,
        update: (Model.Contact.Builder, Boolean) -> Unit,
    ): Model.Contact {
        val contactId = unsafeContactId.sanitized
        val path = contactId.contactPath

        val result = db.mutate { tx ->
            val existingContact = tx.get<Model.Contact>(path)
            val contactBuilder = existingContact?.toBuilder() ?: Model.Contact.newBuilder()
                .setContactId(contactId)
            val isNew = existingContact == null
            if (isNew) {
                contactBuilder.createdTs = now
                contactBuilder.messagesDisappearAfterSeconds =
                    defaultMessagesDisappearAfterSeconds
                contactBuilder.numericFingerprint = numericFingerprintFor(contactId)
            }
            update(contactBuilder, isNew)
            contactBuilder.displayName = contactBuilder.displayName.sanitizedDisplayName
            val contact = contactBuilder.build()
            tx.put(path, contact, fullText = contact.fullText)

            val wasBlocked = existingContact?.blocked == true
            val becameBlocked = !wasBlocked && contact.blocked
            if (becameBlocked) {
                // contact became blocked, delete messages and such
                deleteContactActivity(tx, contact.contactId)
            }

            val verificationLevelChanged =
                existingContact != null &&
                    existingContact.verificationLevel != contact.verificationLevel
            if (verificationLevelChanged) {
                // when verification level changes, update the constrained verification level on all
                // invitations from this contact.
                val introductions =
                    tx.listDetails<Model.StoredMessage>(contactId.introductionMessagesFromQuery)
                introductions.forEach { msg ->
                    val updatedIntroduction = msg.value.introduction.toBuilder()
                        .setConstrainedVerificationLevelValue(
                            msg.value.introduction.verificationLevelValue.coerceAtMost(
                                contact.verificationLevelValue
                            )
                        ).build()
                    tx.put(
                        msg.detailPath,
                        msg.value.toBuilder().setIntroduction(updatedIntroduction).build()
                    )
                }

                // also update the best introductions for all relevant introductions
                tx.listDetails<Model.StoredMessage>(contactId.introductionMessagesFromQuery)
                    .forEach { introduction ->
                        updateBestIntroduction(tx, introduction.value.introduction.to)
                    }
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

        lookupChatNumberIfNecessary(path)
        return result
    }

    /**
     * Looks up a ChatNumber from the short version of the number.
     *
     * @param shortNumber the short number by which to search
     * @param cb callback that gets called with result (or exception if something went wrong)
     */
    fun findChatNumberByShortNumber(
        shortNumber: String,
        cb: (Model.ChatNumber?, Throwable?) -> Unit
    ) {
        anonymousClientWorker.withClient { client ->
            client.findChatNumberByShortNumber(
                shortNumber,
                object : Callback<Messages.ChatNumber> {
                    override fun onSuccess(result: Messages.ChatNumber) {
                        if (!result.number.startsWith(shortNumber)) {
                            cb(
                                null,
                                RuntimeException("Server returned mismatched ChatNumber, this should not happen!") // ktlint-disable max-line-length
                            )
                        } else {
                            cb(result.pbuf, null)
                        }
                    }

                    override fun onError(err: Throwable) {
                        cb(null, err)
                    }
                }
            )
        }
    }

    private fun lookupChatNumberIfNecessary(path: String) {
        db.get<Model.Contact>(path)?.let { contact ->
            if (
                contact.contactId.type == Model.ContactType.DIRECT &&
                contact.chatNumber.number.isEmpty()
            ) {
                val identityKey = ECPublicKey(contact.contactId.id)
                anonymousClientWorker.withClient { client ->
                    client.findChatNumberByIdentityKey(
                        identityKey,
                        object : Callback<Messages.ChatNumber> {
                            override fun onSuccess(result: Messages.ChatNumber) {
                                db.mutate { tx ->
                                    tx.get<Model.Contact>(path)?.let { latestContact ->
                                        if (
                                            ChatNumberEncoding.identityKey(result.number) !=
                                            identityKey ||
                                            !result.number.startsWith(result.shortNumber)
                                        ) {
                                            logger.error("Server returned mismatched ChatNumber. This should not happen!") // ktlint-disable max-line-length
                                        } else {
                                            tx.put(
                                                path,
                                                latestContact.toBuilder().setChatNumber(result.pbuf)
                                                    .build()
                                            )
                                        }
                                    }
                                }
                            }

                            override fun onError(err: Throwable) {
                                if (!(err is TassisError)) {
                                    anonymousClientWorker.retryFailed {
                                        lookupChatNumberIfNecessary(path)
                                    }
                                }
                            }
                        }
                    )
                }
            }
        }
    }

    // Adds a provisional contact.
    //
    // If they're already a contact, this simply sends them a hello but doesn't add a provisional
    // contact.
    @Throws(InvalidKeyException::class)
    fun addProvisionalContact(
        unsafeContactId: String,
        source: Model.ContactSource? = null,
        verificationLevel: Model.VerificationLevel = Model.VerificationLevel.VERIFIED,
    ): ProvisionalContactResult {
        val contactId = unsafeContactId.sanitizedContactId

        val expiresAt = now + provisionalContactsExpireAfterSeconds.secondsToMillis
        val provisionalContactBuilder = Model.ProvisionalContact.newBuilder()
            .setContactId(contactId)
            .setExpiresAt(expiresAt)
            .setVerificationLevel(verificationLevel)
        source?.let { _ -> provisionalContactBuilder.source = source }
        val provisionalContact = provisionalContactBuilder.build()

        var mostRecentHelloTs = 0L
        db.mutate { tx ->
            db.get<Model.Contact>(contactId.directContactPath)?.let {
                mostRecentHelloTs = it.mostRecentHelloTs
            } ?: run {
                tx.put(contactId.provisionalContactPath, provisionalContact)
            }
            sendHello(tx, contactId)
        }

        return ProvisionalContactResult(
            mostRecentHelloTs,
            if (mostRecentHelloTs == 0L) expiresAt else 0
        )
    }

    fun deleteProvisionalContact(unsafeContactId: String) {
        val contactId = unsafeContactId.sanitizedContactId

        // delete provisional contacts in crypto worker's thread to avoid race conditions on
        // managing Signal session
        cryptoWorker.submitForValue {
            doDeleteProvisionalContact(contactId)
        }
    }

    internal fun doDeleteProvisionalContact(contactId: String) {
        db.mutate { tx ->
            tx.delete(contactId.provisionalContactPath)
        }
    }

    /**
     * Deletes the specified contact and all associated data.
     *
     * @param unsafeContactId the base32 encoded public identity key of the contact
     */
    fun deleteDirectContact(unsafeContactId: String) {
        val contactId = unsafeContactId.sanitizedContactId
        deleteContact(contactId.directContactId)
    }

    private fun deleteContact(contactId: Model.ContactId) {
        db.mutate { tx ->
            deleteContactActivity(tx, contactId)
            tx.delete(contactId.contactPath)
        }
    }

    private fun deleteContactActivity(tx: Transaction, contactId: Model.ContactId) {
        tx.list<String>(contactId.contactMessagesQuery)
            .forEach { doDeleteLocally(tx, it.value) }
        db.listPaths(contactId.contactByActivityQuery).forEach {
            tx.delete(it)
        }
        tx.listPaths(contactId.introductionMessagesFromQuery).forEach { path -> tx.delete(path) }
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
     * @param unsafeRecipientId the base32 encoded public identity key of the recipient
     * @param text text for the message
     * @param unsafeReplyToSenderId the id of the sender of the message to which we're replying
     * @param replyToId the id of the message to which we're replying (if replying to a message)
     * @param attachments any attachments to include with the message
     */
    @Throws(IllegalArgumentException::class)
    fun sendToDirectContact(
        unsafeRecipientId: String,
        text: String? = null,
        unsafeReplyToSenderId: String? = null,
        replyToId: String? = null,
        attachments: Array<Model.StoredAttachment>? = null,
        introduction: Model.IntroductionDetails? = null,
    ): Model.StoredMessage {
        val recipientId = unsafeRecipientId.sanitizedContactId
        val replyToSenderId = unsafeReplyToSenderId?.sanitizedContactId

        if (text.isNullOrBlank() && attachments?.size == 0 && introduction == null)
            throw IllegalArgumentException(
                "Please specify either text, an introduction or at least one attachment"
            )
        else if (replyToSenderId.isNullOrBlank() != replyToId.isNullOrBlank()) {
            throw IllegalArgumentException(
                "If specifying either replyToSenderId and replyToId, please specify both"
            )
        }

        val recipient = db.get<Model.Contact>(recipientId.directContactPath)
            ?: throw IllegalArgumentException("Unknown recipient")
        val sendingToSelf = recipientId == myId.id
        val base32Id = randomMessageId.base32
        val sent = now

        val msgBuilder =
            Model.StoredMessage.newBuilder().setId(base32Id)
                .setContactId(recipientId.directContactId)
                .setSenderId(myId.id)
                .setTs(sent)
                .setDirection(Model.MessageDirection.OUT)
        if (recipient.messagesDisappearAfterSeconds > 0) {
            msgBuilder.disappearAfterSeconds = recipient.messagesDisappearAfterSeconds
        }
        text?.let { msgBuilder.setText(it) }
        introduction?.let {
            msgBuilder.introduction = introduction
            // introduction messages don't follow the usual disappearing messages setting
            msgBuilder.disappearAfterSeconds = introductionsDisappearAfterSeconds
        }
        replyToSenderId?.let { msgBuilder.setReplyToSenderId(it) }
        replyToId?.let { msgBuilder.setReplyToId(it) }
        val out = Model.OutboundMessage.newBuilder().setMessageId(base32Id)
            .setSent(sent)
            .setSenderId(myId.id)
            .setRecipientId(recipientId)
        if (sendingToSelf) {
            msgBuilder.direction = Model.MessageDirection.IN
        }

        var attachmentId = 0
        attachments?.forEach { _attachment ->
            val attachment =
                if (sendingToSelf &&
                    _attachment.status == Model.StoredAttachment.Status.PENDING_UPLOAD
                ) {
                    // don't bother uploading attachments on messages sent to ourselves
                    _attachment.toBuilder()
                        .setStatus(Model.StoredAttachment.Status.DONE).build()
                } else {
                    _attachment
                }
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
            tx.put(msg.dbPath, msg, fullText = msg.fullText)
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
     * Send a WebRTC signaling message to a direct contact. Unlike regular messages, these are not
     * persisted on disk and are not queued for send. We attempt to send them immediately, and if
     * that doesn't work for any reason, the return Future will throw an exception.
     *
     * @param unsafeRecipientId the base32 encoded public identity key of the recipient
     * @param content the content of the signal to send
     * @param deviceId optionally, a specific device ID to which to send the signal
     * @param onComplete callback for when sending is complete (whether successful or failed)
     *
     * @return a Future MultiDeviceResult with the result of sending to the relevant devices
     */
    fun sendWebRTCSignal(
        unsafeRecipientId: String,
        content: ByteArray,
        deviceId: String? = null,
        onComplete: (MultiDeviceResult) -> Unit
    ) {
        val recipientId = unsafeRecipientId.sanitizedContactId
        val msg = Model.TransferMessage.newBuilder()
            .setWebRTCSignal(content.byteString()).build()
        cryptoWorker.submit {
            cryptoWorker.sendEphemeral(
                recipientId,
                msg,
                deviceId = deviceId?.let { DeviceId(it) },
                onComplete = onComplete
            )
        }
    }

    /**
     * Subscribes to inbound WebRTC signals.
     *
     * @param subscriberId a unique identifier for this subscription, used in
     *                     #unsubscribeFromWebRTCSignals()
     * @param onSignal callback that gets invoked whenever a signal is received
     */
    fun subscribeToWebRTCSignals(subscriberId: String, onSignal: (WebRTCSignal) -> Unit) {
        webRTCSignalingSubscribers[subscriberId] = onSignal
    }

    /**
     * Unsubscribes the specified subscriber from WebRTC signal notifications.
     *
     * @param subscriberId the ID that was used when calling #subscribeToWebRTCSignals()
     */
    fun unsubscribeFromWebRTCSignals(subscriberId: String) {
        webRTCSignalingSubscribers.remove(subscriberId)
    }

    internal fun notifyWebRTCSignal(
        senderId: String,
        senderDeviceId: String,
        content: ByteString
    ) {
        val signal = WebRTCSignal(senderId, senderDeviceId, content.toByteArray())
        webRTCSignalingSubscribers.values.forEach { subscriber ->
            subscriber(signal)
        }
    }

    /*
     * Re-sends the failed message identified by messageId to all recipients to whom we were unable
     * to send the message.
     *
     * throws IllegalArgumentException if the message couldn't be found, wasn't sent by us, is
     *        already in the process of sending or was already successfully sent.
     */
    @Throws(IllegalArgumentException::class)
    fun resendFailedMessage(messageId: String) {
        db.mutate { tx ->
            tx.get<Model.StoredMessage>(myId.id.storedMessagePath(messageId)).let { msg ->
                if (msg == null) {
                    throw java.lang.IllegalArgumentException("Message not found")
                }
                when (msg.status) {
                    Model.StoredMessage.DeliveryStatus.SENDING ->
                        throw IllegalArgumentException("Message is still in the process of sending")
                    Model.StoredMessage.DeliveryStatus.PARTIALLY_SENT ->
                        throw IllegalArgumentException("Message is still in the process of sending")
                    Model.StoredMessage.DeliveryStatus.COMPLETELY_SENT ->
                        throw IllegalArgumentException("Message was already successfully sent")
                }

                val out = Model.OutboundMessage.newBuilder().setMessageId(messageId)
                    .setSent(now)
                    .setSenderId(myId.id)
                    .setRecipientId(msg.contactId.id)
                tx.put(out.dbPath, out.build())
                cryptoWorker.submit { cryptoWorker.processOutbound(out) }
            }
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
        unsafeContactId: String,
        disappearAfterSeconds: Int
    ) {
        val contactId = unsafeContactId.sanitizedContactId
        val disappearSettings = Model.DisappearSettings.newBuilder()
            .setMessagesDisappearAfterSeconds(disappearAfterSeconds)
            .build().toByteString()
        send(tx, contactId) { out ->
            out.disappearSettings = disappearSettings
        }
    }

    internal fun sendHello(
        tx: Transaction,
        contactId: String,
        final: Boolean = false,
    ) {
        val hello = Model.Hello.newBuilder()
            .setFinal(final)
            .build().toByteString()
        send(tx, contactId) { out ->
            out.hello = hello
        }
    }

    internal fun send(
        tx: Transaction,
        to: String,
        build: (Model.OutboundMessage.Builder) -> Unit,
    ) {
        val out =
            Model.OutboundMessage.newBuilder()
                .setSent(now)
                .setSenderId(myId.id)
                .setRecipientId(to) // TODO: this will need to change for groups
        build(out)
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
     * @param remotelyDeletedBy if this deletion was remotely requested, set the id of the requester
     *                          here. When remotely requested, we retain a record of the message and
     *                          basic metadata, but delete everything else.
     */
    fun deleteLocally(
        msgPath: String,
        remotelyDeletedBy: Model.ContactId? = null
    ): Model.StoredMessage? {
        return db.mutate { tx ->
            doDeleteLocally(tx, msgPath, remotelyDeletedBy)
        }
    }

    private fun doDeleteLocally(
        tx: Transaction,
        msgPath: String,
        remotelyDeletedBy: Model.ContactId? = null
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

            remotelyDeletedBy?.let {
                if (remotelyDeletedBy != msg.contactId) {
                    throw IllegalArgumentException(
                        "Messages can only be remotely deleted by the original sender"
                    )
                }

                // don't actually physically delete the message yet, just clear the message content
                // and mark it as deleted
                tx.put(
                    msg.dbPath,
                    msg.toBuilder()
                        .setRemotelyDeletedBy(remotelyDeletedBy)
                        .setRemotelyDeletedAt(now)
                        .clearText()
                        .clearThumbnails()
                        .clearAttachments()
                        .clearReactions()
                        .build()
                )
            } ?: run {
                // Delete the message
                tx.delete(msgPath)

                // Delete index entries for messages under this Contact's conversation
                tx.delete(msg.contactMessagePath)

                if (msg.hasIntroduction()) {
                    // Delete index entries for introductions
                    tx.delete(msg.senderId.introductionIndexPathByFrom(msg.introduction.to.id))
                    tx.delete(msg.introduction.to.id.introductionIndexPathByTo(msg.senderId))
                    updateBestIntroduction(tx, msg.introduction.to)
                }

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
        val md = Metadata.analyze(file, mimeType)
        val attachment = Model.Attachment.newBuilder().setMimeType(md.mimeType ?: mimeType)
        if (md.additionalMetadata != null) {
            attachment.putAllMetadata(md.additionalMetadata)
        }
        if (metadata != null) {
            attachment.putAllMetadata(metadata)
        }
        val storedAttachment =
            newStoredAttachment.setPlainTextFilePath(file.absolutePath)
                .setAttachment(attachment.build())
        if (md.thumbnail != null) {
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
        length: Long? = null,
        sendingToSelf: Boolean = false
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
        storedAttachment.status =
            if (sendingToSelf)
                Model.StoredAttachment.Status.DONE
            else
                Model.StoredAttachment.Status.PENDING_UPLOAD
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
        val updatedContactBuilder = contact.toBuilder()
            .setMostRecentMessageTs(msg.ts)
            .setMostRecentMessageDirection(msg.direction)
            .setMostRecentMessageText(msg.text)
        if (msg.direction == Model.MessageDirection.IN) {
            updatedContactBuilder.hasReceivedMessage = true
        }
        if (msg.attachmentsCount > 0) {
            updatedContactBuilder.mostRecentAttachmentMimeType =
                msg.attachmentsMap.values.iterator().next().attachment.mimeType
        }
        if (contact.firstReceivedMessageTs == 0L && msg.direction == Model.MessageDirection.IN) {
            updatedContactBuilder.firstReceivedMessageTs = now
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
        val knownAttachmentPaths = mutableListOf<String>()
        db.list<Model.StoredMessage>(Schema.PATH_MESSAGES.path("%"))
            .forEach { msg ->
                knownAttachmentPaths.addAll(
                    msg.value.attachmentsMap.values.map { attachment ->
                        attachment.encryptedFilePath
                    }
                )
                knownAttachmentPaths.addAll(
                    msg.value.attachmentsMap.values.map { attachment ->
                        attachment.thumbnail.encryptedFilePath
                    }
                )
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
            dir.listFiles()?.let { files ->
                for (i in files.indices) {
                    files[i]?.let { file ->
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

    @Throws(IllegalArgumentException::class)
    fun introduce(unsafeRecipientIds: List<String>) {
        doIntroduce(unsafeRecipientIds) { to ->
            Model.IntroductionDetails.newBuilder()
                .setTo(to.contactId)
                .setDisplayName(to.displayName)
                .setVerificationLevel(to.verificationLevel).build()
        }
    }

    /**
     * Like introduce, but accepting a function that builds the introduction, used only for testing.
     */
    internal fun doIntroduce(
        unsafeRecipientIds: List<String>,
        buildIntroduction: (Model.Contact) -> Model.IntroductionDetails
    ) {
        if (unsafeRecipientIds.size < 2) {
            throw IllegalArgumentException("Please specify at least 2 recipients to introduce")
        }

        val recipientIds = unsafeRecipientIds.map { it.sanitizedContactId }

        val introducedParties = recipientIds.map { recipientId ->
            db.get<Model.Contact>(recipientId.directContactPath)
                ?: throw IllegalArgumentException("Unknown recipient")
        }

        db.mutate {
            introducedParties.forEach { a ->
                introducedParties.forEach { b ->
                    if (a.contactId.id != b.contactId.id) {
                        sendToDirectContact(a.contactId.id, introduction = buildIntroduction(b))
                    }
                }
            }
        }
    }

    @Throws(IllegalArgumentException::class)
    fun acceptIntroduction(unsafeFromId: String, unsafeToId: String) {
        val fromId = unsafeFromId.sanitizedContactId
        val toId = unsafeToId.sanitizedContactId

        cryptoWorker.submitForValue {
            doAcceptIntroduction(fromId, toId)
        }
    }

    internal fun doAcceptIntroduction(fromId: String, toId: String) {
        db.mutate { tx ->
            val introductionMessage = tx.introductionMessage(fromId, toId)?.value?.value
                ?: throw IllegalArgumentException("Introduction not found")
            tx.get<Model.Contact>(fromId.directContactPath)
                ?: throw IllegalArgumentException("Introducer not found")

            val introduction = introductionMessage.introduction
            doAddOrUpdateContact(introduction.to.id.directContactId) { contact, isNew ->
                if (isNew) {
                    contact.displayName = introduction.displayName
                    contact.source = Model.ContactSource.INTRODUCTION
                    contact.verificationLevel =
                        introduction.constrainedVerificationLevel
                } else if (
                    introduction.constrainedVerificationLevel > contact.verificationLevel
                ) {
                    // auto-upgrade contact's verification level
                    contact.verificationLevel = introduction.verificationLevel
                }
            }

            // Update status on all introductions
            tx.introductionMessagesTo(toId).forEach { msg ->
                val updatedIntroduction = msg.value.introduction.toBuilder()
                    .setStatus(
                        Model.IntroductionDetails.IntroductionStatus.ACCEPTED
                    )
                    // we update the displayName on all introductions to match the name that we
                    // accepted
                    .setDisplayName(introduction.displayName.sanitizedDisplayName).build()
                tx.put(
                    msg.detailPath,
                    msg.value.toBuilder().setIntroduction(updatedIntroduction).build()
                )
            }

            // Remove "best" introduction
            tx.delete(introduction.to.id.introductionsIndexPathBest)
        }
    }

    fun rejectIntroduction(unsafeFromId: String, unsafeToId: String) {
        val fromId = unsafeFromId.sanitizedContactId
        val toId = unsafeToId.sanitizedContactId

        db.get<String>(fromId.introductionIndexPathByFrom(toId))?.let { path ->
            deleteLocally(path)
        }

        db.get<String>(toId.introductionIndexPathByTo(fromId))?.let { path ->
            deleteLocally(path)
        }
    }

    internal fun updateBestIntroduction(tx: Transaction, to: Model.ContactId) {
        val storedIntros = tx.introductionMessagesTo(to.id).map { it.value }
        val introsByVerificationLevel = sortedSetOf<Model.StoredMessage>(
            { a, b ->
                b.introduction.constrainedVerificationLevelValue -
                    a.introduction.constrainedVerificationLevelValue
            },
            *storedIntros.toTypedArray()
        )
        if (introsByVerificationLevel.isEmpty()) {
            tx.delete(to.id.introductionsIndexPathBest)
        } else {
            val bestIntro = introsByVerificationLevel.first()
            tx.put(bestIntro.introduction.to.id.introductionsIndexPathBest, bestIntro.dbPath)
        }
    }

    /**
     * Searches contacts using the given full-text query.
     */
    fun searchContacts(
        query: String,
        snippetConfig: SnippetConfig = SnippetConfig(numTokens = 10),
    ): List<SearchResult<Model.Contact>> =
        db.search<Model.Contact>(
            Schema.PATH_CONTACTS.path("%"),
            query,
            snippetConfig
        ).map {
            SearchResult(it.path, it.value, it.snippet.split("\n")[0])
        }

    /**
     * Searches messages using the given full-text query.
     */
    fun searchMessages(
        query: String,
        snippetConfig: SnippetConfig = SnippetConfig(numTokens = 10),
    ): List<SearchResult<Model.StoredMessage>> =
        db.search(Schema.PATH_MESSAGES.path("%"), query, snippetConfig)

    internal fun numericFingerprintFor(contactId: Model.ContactId): String =
        NumericFingerprintGenerator(fingerprintIterations)
            .createFor(
                0,
                emptyBytes,
                listOf(ECPublicKey(myId.id)),
                emptyBytes,
                listOf(ECPublicKey(contactId.id))
            ).displayableFingerprint.displayText

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
            logger.error("error closing Messaging: ${t.message}", t)
        }
    }
}

internal val randomMessageId: ByteString
    get() {
        val uuid = UUID.randomUUID()
        val bytes = ByteArray(16)
        val bb = ByteBuffer.wrap(bytes)
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        return ByteString.copyFrom(bytes)
    }

/**
 * The UNIX timestamp corresponding to the current instant in time, in milliseconds
 */
val now: Long
    get() = System.currentTimeMillis()

/**
 * Sanitizes string contact ids that are supposed to be in human-friendly Base32 encoding.
 * Human-friendly base32 encoding supports substitutions for certain characters (for example, o is
 * replaced by 0) and it also assumes everything is lowercase. So, this function converts everything
 * to lowercase and round trips it through ECPublicKey so that we end up with the canonical human-
 * friendly representation. That avoids scenarios like having to contact entries at different
 * database paths that are in fact the same contact id, like '.......o.....' and '.......0.....'.
 *
 * This sanitizing should be performed anywhere that a contact ID is passed in externally, either
 * from user input or through a message received from another user such as an Introduction.
 *
 * @throws InvalidKeyException if the ID doesn't decode to the right length (52 bytes)
 */
val String.sanitizedContactId: String
    @Throws(InvalidKeyException::class)
    get() {
        return ECPublicKey(this.lowercase(Locale.ROOT).trim()).toString()
    }

/**
 * Applies #String.sanitizedContactId to a Model.ContactId.
 */
val Model.ContactId.sanitized: Model.ContactId
    @Throws(InvalidKeyException::class)
    get() {
        return toBuilder().setId(id.sanitizedContactId).build()
    }

/**
 * This regex matches any character that's not a letter, number or the space character
 */
internal val invalidDisplayNameCharacters = Regex("[^\\p{L}\\p{N} ]")

/**
 * This regex matches 2 or more spaces in a row
 */
internal val multipleSpaces = Regex(" +")

val String.sanitizedDisplayName: String
    get() {
        return this.replace(invalidDisplayNameCharacters, "")
            .replace(multipleSpaces, " ")
            .trim()
    }

val Model.StoredAttachment.inputStream: InputStream
    get() = AttachmentCipherInputStream.createForAttachment(
        File(encryptedFilePath),
        attachment.plaintextLength,
        attachment.keyMaterial.toByteArray(),
        attachment.digest.toByteArray()
    )

private val maxSha1Hash = BigInteger.valueOf(2).pow(160)

/**
 * Calculates a SHA1 hash of the string's UTF-8 representation, coerced to a scaled integer between
 * 0 and max inclusive.
 */
fun String.sha1(max: Long): Long {
    val bytes = MessageDigest.getInstance("SHA-1").digest(this.toByteArray(Charsets.UTF_8))
    return BigInteger(1, bytes).times(BigInteger.valueOf(max)).div(maxSha1Hash).toLong()
}
