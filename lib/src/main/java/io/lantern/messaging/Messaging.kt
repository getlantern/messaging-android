package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.Subscriber
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.*
import mu.KotlinLogging
import org.signal.libsignal.metadata.SealedSessionCipher
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SessionBuilder
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.ecc.ECKeyPair
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.state.PreKeyBundle
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import java.io.Closeable
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.pow
import kotlin.time.*


@ExperimentalTime
class Messaging(
    internal val store: MessagingStore,
    private val transportFactory: TransportFactory,
    private val redialBackoff: Duration = 50.milliseconds,
    private val maxRedialDelay: Duration = 15.seconds,
    failedSendRetryDelay: Duration = 5.seconds,
    private val stopSendRetryAfter: Duration = 10.minutes,
    numInitialPreKeysToRegister: Int = 5,
    private val name: String = "messaging",
) : Closeable {
    private val logger = KotlinLogging.logger(name)

    // All processing that involves crypto and client operations happens on this executor to keep
    // SignalProtocolStore in a consistent state
    private val executor = Executors.newSingleThreadScheduledExecutor() {
        Thread(it, "${name}-executor")
    }
    private var anonymousClient: AnonymousClient? = null
    private var authenticatedClient: AuthenticatedClient? = null

    private val identityKeyPair: ECKeyPair
    private val deviceId: DeviceId
    private val cipher = SealedSessionCipher(store, store.deviceId)

    private val subscriberId = UUID.randomUUID().toString()

    init {
        store.db.registerType(21, Model.Contact::class.java)
        store.db.registerType(22, Model.Conversation::class.java)
        store.db.registerType(23, Model.ShortMessageRecord::class.java)
        store.db.registerType(24, Model.OutgoingShortMessage::class.java)

        identityKeyPair = store.identityKeyPair
        deviceId = store.deviceId

        // on startup, register some pre keys
        registerPreKeys(numInitialPreKeysToRegister)

        // listen for outbound messages (including pulling any previously unprocessed outbound
        // messages)
        store.db.subscribe(object :
            Subscriber<Model.OutgoingShortMessage>(
                subscriberId,
                "${Schema.PATH_OUTBOUND}/"
            ) {
            override fun onUpdate(path: String, value: Model.OutgoingShortMessage) {
                val timeSinceFailure = System.currentTimeMillis() * 1000000 - value.lastFailed
                val delayNanos = (failedSendRetryDelay.toLongNanoseconds() - timeSinceFailure)
                encryptAndSend(value, delayMillis = delayNanos / 1000000)
            }

            override fun onDelete(path: String) {
                // not interested
            }
        })
    }

    /**
     * Send an outbound message from the user
     */
    fun send(
        text: String?,
        oggVoice: ByteArray?,
        vararg recipients: ECPublicKey
    ): Model.ShortMessageRecord {
        if (text == null && oggVoice == null) {
            throw IllegalArgumentException("Please specify either text or oggVoice")
        } else if (text != null && oggVoice != null) {
            throw IllegalArgumentException("Please specify either text or oggVoice but not both")
        }
        if (recipients.isEmpty()) {
            throw IllegalArgumentException("Please specify at least one recipient")
        }
        val shortMessageBuilder = Model.ShortMessage.newBuilder().setId(randomMessageId).setSent(
            nowUnixNano
        )
        if (text != null) {
            shortMessageBuilder.setText(text)
        } else {
            shortMessageBuilder.setOggVoice(oggVoice?.byteString())
        }
        val msg = shortMessageBuilder.build()
        val outgoingMessage = Model.OutgoingShortMessage.newBuilder()
            .addAllRecipients(recipients.map { it.bytes.byteString() }).setMessage(msg).build()
        val messageRecord =
            Model.ShortMessageRecord.newBuilder().setId(msg.id.base32).setSent(msg.sent)
                .setMessage(msg.toByteString()).setDirection(Model.ShortMessageRecord.Direction.OUT)
                .setStatus(Model.ShortMessageRecord.DeliveryStatus.UNSENT).build()
        store.db.mutate { tx ->
            // save the message in a list of all messages
            tx.put(messageRecord.dbPath, messageRecord)
            // save a pointer to the message under each recipient conversation
            outgoingMessage.recipientsList.forEach { recipient ->
                tx.put(
                    messageRecord.conversationMessagePath(recipient),
                    messageRecord.dbPath
                )
                val existingConversation =
                    tx.findOne<Model.Conversation>(outgoingMessage.conversationsQuery)
                if (existingConversation != null) {
                    tx.delete(existingConversation.dbPath)
                }
                val conversation = Model.Conversation.newBuilder()
                    .addIdentityKeys(recipient)
                    .setDisplayName(existingConversation?.displayName ?: "")
                    .setMostRecentMessageTime(msg.sent)
                    .setMostRecentMessageText(msg.text).build()
                tx.put(conversation.dbPath, conversation)
            }
            // enqueue the outgoing message in the db for sending (actual send happens in the
            // message processing loop)
            tx.put(messageRecord.outboundPath, outgoingMessage)
        }
        return messageRecord
    }

    private fun registerPreKeys(numPreKeys: Int, delayMillis: Long = 0) {
        schedule(delayMillis, TimeUnit.MILLISECONDS) {
            val client = getAuthenticatedClient()
            try {
                store.db.mutate {
                    val spk = store.nextSignedPreKey
                    val otpks = store.generatePreKeys(numPreKeys)
                    client.register(spk.serialize(), otpks.map { it.serialize() })
                }
            } catch (t: Throwable) {
                logger.debug(
                    "unable to register pre keys, will try again later: ${t.message}",
                    t
                )
                registerPreKeys(numPreKeys, 5000)
            }
        }
    }

    private fun encryptAndSend(outgoingMessage: Model.OutgoingShortMessage, delayMillis: Long) {
        schedule(delayMillis, TimeUnit.MILLISECONDS) {
            val client = getAnonymousClient()
            val unsentRecipients = HashSet<ByteString>(outgoingMessage.recipientsList)
            outgoingMessage.recipientsList.forEach { recipient ->
                try {
                    encryptAndSendTo(client, outgoingMessage, recipient.toByteArray())
                    unsentRecipients.remove(recipient)
                } catch (t: Throwable) {
                    logger.debug("error sending, will retry: ${t.message}")
                }
            }
            val successful = unsentRecipients.size == 0
            val permanentlyFailed =
                !successful && outgoingMessage.lastFailed > 0 && System.currentTimeMillis() * 1000000 - outgoingMessage.lastFailed > stopSendRetryAfter.toLongNanoseconds()
            val deliveryStatus = if (successful) {
                Model.ShortMessageRecord.DeliveryStatus.SENT
            } else if (permanentlyFailed) {
                if (unsentRecipients.size < outgoingMessage.recipientsList.size) {
                    Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_FAILED
                } else {
                    Model.ShortMessageRecord.DeliveryStatus.PARTIALLY_FAILED
                }
            } else {
                Model.ShortMessageRecord.DeliveryStatus.FAILING
            }
            val userMessage =
                outgoingMessage.message.outbound(deliveryStatus)
            store.db.mutate { tx ->
                tx.put(userMessage.dbPath, userMessage)
                if (successful || permanentlyFailed) {
                    tx.delete(userMessage.outboundPath)
                } else {
                    val outbound =
                        outgoingMessage.toBuilder().clearRecipients()
                            .addAllRecipients(unsentRecipients.toList())
                            .setLastFailed(System.currentTimeMillis() * 1000000)
                            .build()
                    tx.put(userMessage.outboundPath, outbound)
                }
            }
        }
    }

    private fun encryptAndSendTo(
        client: AnonymousClient,
        msg: Model.OutgoingShortMessage,
        recipient: ByteArray
    ) {
        // run encryption and sending in a single transaction so that if any part fails, our session states roll back
        store.db.mutate { tx ->
            val recipientIdentityKey = ECPublicKey(recipient)
            val knownDeviceIds =
                store.getSubDeviceSessions(recipientIdentityKey.toString())
            val deviceIds = if (knownDeviceIds.size == 0) {
                // no known devices, try fetching pre-keys
                val preKeys = client.retrievePreKeys(recipientIdentityKey, knownDeviceIds)
                preKeys.forEach { preKey ->
                    val signedPreKey = SignedPreKeyRecord(preKey.signedPreKey.toByteArray())
                    val oneTimePreKey = PreKeyRecord(preKey.oneTimePreKey.toByteArray())
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
                val transferMsg =
                    Model.TransferMessage.newBuilder().setShortMessage(msg.message).build()
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
        submit {
            store.db.mutate { tx ->
                val decryptionResult = cipher.decrypt(msg.data.toByteArray())
                val plainText = Padding.stripMessagePadding(decryptionResult.paddedMessage)
                val transferMsg = Model.TransferMessage.parseFrom(plainText)
                val sender = decryptionResult.senderAddress
                val msg = transferMsg.shortMessage.inbound()
                // save the message in a list of all messages
                // note - we use putIfAbsent to avoid overwriting previously received messages with
                // the same ID. As long as the sender isn't malfunctioning or being malicious, this
                // should never happen.
                if (tx.putIfAbsent(msg.dbPath, msg)) {
                    // save a pointer to the message under the conversation message path
                    tx.put(msg.conversationMessagePath(sender.identityKey), msg)
                }
            }
            msg.ack()
        }
    }

    private fun schedule(delay: Long, unit: TimeUnit, cmd: () -> Unit) {
        executor.schedule({
            try {
                cmd()
            } catch (t: Throwable) {
                logger.error(t.message, t)
            }
        }, delay, unit)
    }

    private fun submit(cmd: () -> Unit) {
        executor.submit {
            try {
                cmd()
            } catch (t: Throwable) {
                logger.error(t.message, t)
            }
        }
    }

    @Synchronized
    private fun getAnonymousClient(): AnonymousClient {
        val existing = anonymousClient
        if (existing != null) {
            return existing
        }

        var failures = 0
        while (true) {
            try {
                val newClient = AnonymousClient.connect(transportFactory, object : ClientDelegate {
                    override fun onClose(err: Throwable?) {
                        synchronized(this) {
                            anonymousClient = null
                        }
                    }
                })
                anonymousClient = newClient
                return newClient
            } catch (t: Throwable) {
                val redialDelay = redialBackoff * 2.0.pow(failures)
                val actualRedialDelay =
                    if (maxRedialDelay < redialDelay) maxRedialDelay else redialDelay
                logger.error(
                    "error dialing tassis, will retry in ${actualRedialDelay}: ${t.message}",
                    t
                )
                Thread.sleep(actualRedialDelay.toLongMilliseconds())
                failures++
            }
        }
    }

    @Synchronized
    private fun getAuthenticatedClient(): AuthenticatedClient {
        val existing = authenticatedClient
        if (existing != null) {
            return existing
        }

        var failures = 0
        while (true) {
            try {
                val newClient = AuthenticatedClient.connect(
                    transportFactory,
                    identityKeyPair.publicKey,
                    deviceId,
                    object : AuthenticatedClientDelegate {
                        override fun signLogin(loginBytes: ByteArray) = Curve.calculateSignature(
                            identityKeyPair.privateKey,
                            loginBytes
                        )

                        override fun onPreKeysLow(numPreKeysRequested: Int) {
                            registerPreKeys(numPreKeysRequested)
                        }

                        override fun onInboundMessage(msg: InboundMessage) {
                            decryptAndStore(msg)
                        }

                        override fun onClose(err: Throwable?) {
                            synchronized(this) {
                                authenticatedClient = null
                            }
                        }
                    })
                authenticatedClient = newClient
                return newClient
            } catch (t: Throwable) {
                val redialDelay = redialBackoff * 2.0.pow(failures)
                val actualRedialDelay =
                    if (maxRedialDelay < redialDelay) maxRedialDelay else redialDelay
                logger.error(
                    "error dialing tassis, will retry in ${actualRedialDelay}: ${t.message}",
                    t
                )
                Thread.sleep(actualRedialDelay.toLongMilliseconds())
                failures++
            }
        }
    }

    override fun close() {
        executor.shutdownNow()
        synchronized(this) {
            anonymousClient?.close()
            anonymousClient = null
            authenticatedClient?.close()
            authenticatedClient = null
        }
        executor.awaitTermination(10, TimeUnit.SECONDS)
        store.db.unsubscribe(subscriberId)
        store.close()

    }
}

val randomMessageId: ByteString
    get() {
        val uuid = UUID.randomUUID()
        val bb = ByteBuffer.wrap(ByteArray(16))
        bb.putLong(uuid.mostSignificantBits)
        bb.putLong(uuid.leastSignificantBits)
        return ByteString.copyFrom(bb)
    }

val nowUnixNano: Long
    get() = System.currentTimeMillis() * 1000000

