package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.*
import io.lantern.observablemodel.Subscriber
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select
import mu.KotlinLogging
import org.signal.libsignal.metadata.SealedSessionCipher
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SessionBuilder
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.state.PreKeyBundle
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import org.whispersystems.libsignal.util.Base32
import java.io.Closeable
import java.util.*
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class Messaging(
    internal val store: MessagingStore,
    private val scope: CoroutineScope,
    private val redialBackoff: Duration = 50.milliseconds,
    private val maxRedialDelay: Duration = 15.seconds,
    private val numInitialPreKeysToRegister: Int = 5,
    private val dialTassis: suspend (CoroutineScope) -> ClientConnection
) : Closeable {

    private val outboundUserMessages =
        Channel<Model.OutboundUserMessage>(Channel.UNLIMITED)
    private val registerPreKeysIfNecessary = Channel<Int>(1)

    private val cipher = SealedSessionCipher(store, store.deviceId)

    private val subscriberId = UUID.randomUUID().toString()
    private val job: Job

    init {
        store.db.registerType(21, Model.Contact::class.java)
        store.db.registerType(22, Model.UserMessage::class.java)
        store.db.registerType(23, Model.Attachment::class.java)
        store.db.registerType(24, Model.OutboundUserMessage::class.java)


        // do all processing that involves crypto operations on a single coroutine to keep
        // SignalProtocolStore in a consistent state
        job = scope.launch {
            // on startup, register pre keys if necessary
            registerPreKeysIfNecessary.send(numInitialPreKeysToRegister)

            // listen for outbound messages (including pulling any previously unprocessed outbound
            // messages)
            store.db.subscribe(object :
                Subscriber<Model.OutboundUserMessage>(
                    subscriberId,
                    "${Schema.PATH_OUTBOUND}/"
                ) {
                override fun onUpdate(path: String, value: Model.OutboundUserMessage) {
                    logger.debug("onUpdate.begin")
                    runBlocking(Dispatchers.IO) {
                        logger.debug("onUpdate.inner.being")
                        outboundUserMessages.send(value)
                        logger.debug("onUpdate.inner.end")
                    }
                    logger.debug("onUpdate.end")
                }

                override fun onDelete(path: String) {
                    // not interested
                }
            })

            while (isActive) {
                try {
                    coroutineScope {
                        dialServer().use { anonymousConn ->
                            dialServer().use { authenticatedConn ->
                                val client = Client(
                                    store.identityKeyPair.publicKey,
                                    store.deviceId,
                                    anonymousConn,
                                    authenticatedConn,
                                    scope
                                ) { loginBytes ->
                                    Curve.calculateSignature(
                                        store.identityKeyPair.privateKey,
                                        loginBytes
                                    )
                                }

                                while (isActive) {
                                    processMessages(client)
                                }
                            }
                        }
                    }
                } catch (t: Throwable) {
                    logger.error("Error processing messages: ${t.message}", t)
                }
            }
        }
    }

    private suspend fun dialServer(): ClientConnection {
        var failures = 0
        while (true) {
            try {
                return dialTassis(scope)
            } catch (t: Throwable) {
                val redialDelay = redialBackoff * 2.0.pow(failures)
                val actualRedialDelay =
                    if (maxRedialDelay < redialDelay) maxRedialDelay else redialDelay
                logger.error(
                    "error dialing tassis, will retry in ${actualRedialDelay}: ${t.message}",
                    t
                )
                delay(actualRedialDelay.toLongMilliseconds())
                failures++
            }
        }
    }

    private suspend fun processMessages(client: Client) {
        select<Unit> {
            client.preKeysLow.onReceive {
                registerPreKeys(client, it)
            }
            outboundUserMessages.onReceive {
                encryptAndSend(client, it)
            }
            client.inbound.onReceive {
                decryptAndStore(it)
            }
        }
    }

    private suspend fun registerPreKeys(client: Client, numPreKeys: Int) {
        val spk = store.nextSignedPreKey
        val otpks = store.generatePreKeys(numPreKeys)
        client.registerPreKeys(Model.SignedPreKey.newBuilder().setId(spk.id)
            .setPublicKey(spk.keyPair.publicKey.bytes.byteString())
            .setSignature(spk.signature.byteString()).build().toByteArray(),
            otpks.map { otpk ->
                Model.OneTimePreKey.newBuilder().setId(otpk.id)
                    .setPublicKey(otpk.keyPair.publicKey.bytes.byteString()).build()
                    .toByteArray()
            }
        )
    }

    private suspend fun encryptAndSend(client: Client, msg: Model.OutboundUserMessage) {
        val unsentRecipients = HashSet<ByteString>(msg.recipientsList)
        msg.recipientsList.forEach { recipient ->
            try {
                encryptAndSendTo(client, msg, recipient.toByteArray())
                unsentRecipients.remove(recipient)
            } catch (t: Throwable) {
                logger.error("error sending, will retry: ${t.message}", t)
            }
        }
        val successful = unsentRecipients.size == 0
        val userMessage =
            msg.content.outbound(if (successful) Model.DeliveryStatus.SENT else Model.DeliveryStatus.FAILING)
        store.db.smutate { tx ->
            logger.debug("storing user message at ${userMessage.dbPath}")
            tx.put(userMessage.dbPath, userMessage)
            if (successful) {
                tx.delete(userMessage.outboundPath)
            } else {
                val outbound =
                    msg.toBuilder().clearRecipients().addAllRecipients(unsentRecipients.toList())
                        .build()
                tx.put(userMessage.outboundPath, outbound)
            }
        }
    }

    private suspend fun encryptAndSendTo(
        client: Client,
        msg: Model.OutboundUserMessage,
        recipient: ByteArray
    ) {
        // run encryption and sending in a single transaction so that if any part fails, our session states roll back
        store.db.smutate { tx ->
            val recipientIdentityKey = ECPublicKey(recipient)
            val knownDeviceIds =
                store.getSubDeviceSessions(recipientIdentityKey.toString())
            val deviceIds = if (knownDeviceIds.size == 0) {
                // no known devices, try fetching pre-keys
                val preKeys = client.retrievePreKeys(recipientIdentityKey, knownDeviceIds)
                preKeys.forEach { preKey ->
                    val oneTimePreKey = PreKeyRecord(preKey.oneTimePreKey.toByteArray())
                    val signedPreKey = SignedPreKeyRecord(preKey.signedPreKey.toByteArray())
                    // TODO: implement max_recv checking for signed pre key age
                    val builder = SessionBuilder(
                        store,
                        SignalProtocolAddress(
                            recipientIdentityKey,
                            DeviceId(preKey.deviceId.toByteArray())
                        )
                    )
                    builder.process(
                        PreKeyBundle(
                            oneTimePreKey.id,
                            oneTimePreKey.keyPair.publicKey,
                            signedPreKey.id,
                            signedPreKey.keyPair.publicKey,
                            signedPreKey.signature,
                            recipientIdentityKey
                        )
                    )
                }
                preKeys.map { DeviceId(it.deviceId.toByteArray()) }
            } else {
                knownDeviceIds
            }

            deviceIds.forEach { deviceId ->
                val transferMsg = Model.TransferMessage.newBuilder().setUserMessage(msg.content)
                    .addAllAttachments(msg.attachmentsList).build()
                // TODO: we (mostly Signal) use ByteArray everywhere, but Protocol Buffers wants byte strings
                // which have to be copied from the ByteArray. That results in a lot of extra copies,
                // it would  sure be nice to avoid that.
                val plainText = transferMsg.toByteArray()
                val paddedPlainText = Padding.padMessage(plainText)
                val to = SignalProtocolAddress(recipientIdentityKey, deviceId)
                val unidentifiedSenderMessage: ByteArray =
                    cipher.encrypt(to, paddedPlainText)
                client.sendUnidentifiedSenderMessage(to, unidentifiedSenderMessage)
            }
        }
    }

    private suspend fun decryptAndStore(msg: InboundMessage) {
        store.db.smutate { tx ->
            val decryptionResult = cipher.decrypt(msg.data.toByteArray())
            val plainText = Padding.stripMessagePadding(decryptionResult.paddedMessage)
            val transferMsg = Model.TransferMessage.parseFrom(plainText)
            val sender = decryptionResult.senderAddress
            val userMessage = transferMsg.userMessage.inbound()
            // save the message in a list of all messages
            tx.put(userMessage.dbPath, userMessage)
            // save each attachment
            transferMsg.attachmentsList.forEach {
                tx.put(userMessage.attachmentPath(it), it)
            }
            // save a pointer to the message under the sender
            tx.put(userMessage.contactMessagePath(sender.identityKey), userMessage)
        }
        msg.ack()
    }

    suspend fun addContact(contact: Model.Contact) {
        store.db.smutate { tx ->
            tx.put(contact.address.contactPath, contact)
        }
    }

    /**
     * Send an outbound message from the user
     */
    suspend fun send(msg: Model.OutboundUserMessage) {
        val userMessage = msg.content.outbound(Model.DeliveryStatus.UNSENT)
        store.db.smutate { tx ->
            // save the message in a list of all messages
            tx.put(userMessage.dbPath, userMessage)
            // save each attachment
            msg.attachmentsList.forEach {
                tx.put(userMessage.attachmentPath(it), it)
            }
            // save a pointer to the message under each recipient
            msg.recipientsList.forEach { recipient ->
                tx.put(
                    userMessage.contactMessagePath(recipient),
                    userMessage.dbPath
                )
            }
            // enqueue the message in the db for sending (actual send happens in the message
            // processing loop)
            tx.put(userMessage.outboundPath, msg)
        }
    }

    override fun close() {
        job.cancel()
        store.close()
        store.db.unsubscribe(subscriberId)
    }
}

object Schema {
    const val PATH_USER_MESSAGES = "/usermessages"
    const val PATH_ATTACHMENTS =
        "/attachments" // TODO: maybe store attachments in a different table from the main one
    const val PATH_OUTBOUND = "/outbound"
    const val PATH_CONTACTS = "/contacts"
    const val PATH_CONTACT_MESSAGES = "/contactmessages"
}

fun Model.UserMessageContent.outbound(status: Model.DeliveryStatus): Model.UserMessage {
    return Model.UserMessage.newBuilder().setId(this.id.base32).setSent(this.sent)
        .setDirection(Model.UserMessage.Direction.OUTBOUND).setStatus(status)
        .setContent(this.toByteString()).build()
}

fun Model.UserMessageContent.inbound(): Model.UserMessage {
    return Model.UserMessage.newBuilder().setId(this.id.base32).setSent(this.sent)
        .setDirection(Model.UserMessage.Direction.INBOUND)
        .setContent(this.toByteString()).build()
}

val ByteArray.base32: String get() = Base32.humanFriendly.encodeToString(this)

val ByteString.base32: String get() = Base32.humanFriendly.encodeToString(this.toByteArray())

fun String.path(vararg elements: Any): String {
    val builder = StringBuilder(this)
    elements.forEach {
        builder.append("/")
        builder.append(
            when (it) {
                is ByteArray -> it.base32
                is ByteString -> it.base32
                is ECPublicKey -> it.bytes.base32
                is DeviceId -> it.bytes.base32
                else -> it
            }
        )
    }
    return builder.toString()
}

val Model.UserMessage.dbPath: String
    get() = Schema.PATH_USER_MESSAGES.path(this.sent, this.id)

val Model.UserMessageContent.dbPath: String
    get() = Schema.PATH_USER_MESSAGES.path(this.sent, this.id)

val Model.UserMessage.outboundPath: String
    get() = Schema.PATH_OUTBOUND.path(this.sent, this.id)

fun Model.UserMessage.contactMessagePath(identityKey: Any): String =
    Schema.PATH_CONTACT_MESSAGES.path(identityKey, this.sent, this.id)

fun Model.UserMessage.attachmentPath(attachment: Model.Attachment): String =
    Schema.PATH_ATTACHMENTS.path(this.id, attachment.id)

val Model.Address.contactPath: String
    get() = Schema.PATH_CONTACTS.path(this.identityKey, this.deviceId)

