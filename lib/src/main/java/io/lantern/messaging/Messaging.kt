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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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
    failedSendRetryDelay: Duration = 5.seconds,
    numInitialPreKeysToRegister: Int = 5,
    private val name: String = "messaging",
    private val dialTassis: suspend (CoroutineScope) -> ClientConnection
) : Closeable {
    // All processing that involves crypto operations happens on this executor to keep
    // SignalProtocolStore in a consistent state
    private val executor = Executors.newScheduledThreadPool(8) {
        Thread(it, "${name}-executor")
    }
    private val clientChannel = Channel<Client>(1)
    private val cipher = SealedSessionCipher(store, store.deviceId)

    private val subscriberId = UUID.randomUUID().toString()
    private val job: Job

    init {
        store.db.registerType(21, Model.Contact::class.java)
        store.db.registerType(22, Model.UserMessage::class.java)
        store.db.registerType(23, Model.Attachment::class.java)
        store.db.registerType(24, Model.OutboundUserMessage::class.java)

        val identityKeyPair = store.identityKeyPair

        // on startup, register some pre keys
        registerPreKeys(numInitialPreKeysToRegister)

        // listen for outbound messages (including pulling any previously unprocessed outbound
        // messages)
        store.db.subscribe(object :
            Subscriber<Model.OutboundUserMessage>(
                subscriberId,
                "${Schema.PATH_OUTBOUND}/"
            ) {
            override fun onUpdate(path: String, value: Model.OutboundUserMessage) {
                val timeSinceFailure = System.currentTimeMillis() * 1000000 - value.lastFailed
                val delayNanos = (failedSendRetryDelay.toLongNanoseconds() - timeSinceFailure)
                encryptAndSend(value, delayMillis = delayNanos / 1000000)
            }

            override fun onDelete(path: String) {
                // not interested
            }
        })

        // maintain client connections
        job = scope.launch {
            while (isActive) {
                try {
                    coroutineScope {
                        val anonymousConn = dialServer()
                        val authenticatedConn = dialServer()
                        val client = Client(
                            identityKeyPair.publicKey,
                            store.deviceId,
                            anonymousConn,
                            authenticatedConn,
                            scope
                        ) { loginBytes ->
                            Curve.calculateSignature(
                                identityKeyPair.privateKey,
                                loginBytes
                            )
                        }
                        clientChannel.send(client)

                        while (isActive) {
                            processInboundFromClient(client)
                        }
                    }
                } catch (t: Throwable) {
                    logger.error("Error maintaining client connections: ${t.message}", t)
                }
            }
        }
    }

    fun addContact(contact: Model.Contact) {
        store.db.mutate { tx ->
            tx.put(contact.address.contactPath, contact)
        }
    }

    /**
     * Send an outbound message from the user
     */
    fun send(msg: Model.OutboundUserMessage) {
        executor.submit {
            val userMessage = msg.content.outbound(Model.DeliveryStatus.UNSENT)
            logger.debug("sending")
            store.db.mutate { tx ->
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
    }

    private suspend fun processInboundFromClient(client: Client) {
        select<Unit> {
            client.preKeysLow.onReceive {
                registerPreKeys(it)
            }
            client.inbound.onReceive {
                decryptAndStore(it)
            }
        }
    }

    private fun registerPreKeys(numPreKeys: Int, delayMillis: Long = 0) {
        executor.schedule({
            getClient().use { ch ->
                try {
                    logger.debug("registering ${numPreKeys} pre keys")
                    store.db.mutate {
                        val spk = store.nextSignedPreKey
                        val otpks = store.generatePreKeys(numPreKeys)
                        logger.debug("about to call client.registerPeKeys")
                        ch.client.registerPreKeys(Model.SignedPreKey.newBuilder().setId(spk.id)
                            .setPublicKey(spk.keyPair.publicKey.bytes.byteString())
                            .setSignature(spk.signature.byteString()).build().toByteArray(),
                            otpks.map { otpk ->
                                Model.OneTimePreKey.newBuilder().setId(otpk.id)
                                    .setPublicKey(otpk.keyPair.publicKey.bytes.byteString()).build()
                                    .toByteArray()
                            }
                        )
                        logger.debug("done mutating")
                    }
                    logger.debug("registered ${numPreKeys} pre keys")
                } catch (t: Throwable) {
                    logger.error(
                        "unable to register pre keys, will try again later: ${t.message}",
                        t
                    )
                    registerPreKeys(numPreKeys, 5000)
                }
            }
        }, delayMillis, TimeUnit.MILLISECONDS)
    }

    private fun encryptAndSend(msg: Model.OutboundUserMessage, delayMillis: Long) {
        executor.schedule({
            getClient().use { ch ->
                val unsentRecipients = HashSet<ByteString>(msg.recipientsList)
                msg.recipientsList.forEach { recipient ->
                    try {
                        encryptAndSendTo(ch.client, msg, recipient.toByteArray())
                        unsentRecipients.remove(recipient)
                    } catch (t: Throwable) {
                        logger.error("error sending, will retry: ${t.message}", t)
                    }
                }
                val successful = unsentRecipients.size == 0
                val userMessage =
                    msg.content.outbound(if (successful) Model.DeliveryStatus.SENT else Model.DeliveryStatus.FAILING)
                logger.debug("encryptAndSend result")
                store.db.mutate { tx ->
                    tx.put(userMessage.dbPath, userMessage)
                    if (successful) {
                        tx.delete(userMessage.outboundPath)
                    } else {
                        val outbound =
                            msg.toBuilder().clearRecipients()
                                .addAllRecipients(unsentRecipients.toList())
                                .setLastFailed(System.currentTimeMillis() * 1000000)
                                .build()
                        tx.put(userMessage.outboundPath, outbound)
                    }
                }
            }
        }, delayMillis, TimeUnit.MILLISECONDS)
    }

    private fun encryptAndSendTo(
        client: Client,
        msg: Model.OutboundUserMessage,
        recipient: ByteArray
    ) {
        // run encryption and sending in a single transaction so that if any part fails, our session states roll back
        logger.debug("encryptAndSendTo")
        store.db.mutate { tx ->
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

    private fun decryptAndStore(msg: InboundMessage) {
        store.db.mutate { tx ->
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

    internal fun getClient(): ClientHolder {
        return runBlocking {
            ClientHolder(clientChannel.receive(), clientChannel)
        }
    }

    override fun close() {
        job.cancel()
        store.close()
        store.db.unsubscribe(subscriberId)
        executor.shutdownNow()
        clientChannel.poll()?.close()
    }
}

internal class ClientHolder(val client: Client, private val clientChannel: Channel<Client>) :
    Closeable {
    override fun close() {
        runBlocking {
            clientChannel.send(client)
        }
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

