package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.Subscriber
import io.lantern.db.Transaction
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.*
import io.lantern.messaging.time.millisToNanos
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.nanosToMillis
import io.lantern.messaging.time.secondsToMillis
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
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.math.pow

class UnknownSenderException : Exception("Unknown sender")

class Messaging(
    internal val store: MessagingStore,
    private val transportFactory: TransportFactory,
    private val redialBackoffMillis: Long = 50L.secondsToMillis,
    private val maxRedialDelayMillis: Long = 15L.secondsToMillis,
    private val failedSendRetryDelayMillis: Long = 5L.secondsToMillis,
    private val stopSendRetryAfterMillis: Long = 10L.minutesToMillis,
    numInitialPreKeysToRegister: Int = 5,
    private val name: String = "messaging",
) : Closeable {
    private val logger = KotlinLogging.logger(name)

    // All processing that involves crypto and client operations happens on this executor to keep
    // SignalProtocolStore in a consistent state
    private val executor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${name}-executor")
    }
    private val anonymousClientLock = Semaphore(1)
    private var anonymousClientConnectFailures = 0
    private var anonymousClient: AnonymousClient? = null
    private val authenticatedClientLock = Semaphore(1)
    private var authenticatedClientConnectFailures = 0
    private var authenticatedClient: AuthenticatedClient? = null

    private val identityKeyPair: ECKeyPair
    private val deviceId: DeviceId
    private val cipher = SealedSessionCipher(store, store.deviceId)

    private val subscriberId = UUID.randomUUID().toString()

    init {
        db.registerType(21, Model.Contact::class.java)
        db.registerType(22, Model.ShortMessageRecord::class.java)
        db.registerType(23, Model.OutgoingShortMessage::class.java)

        identityKeyPair = store.identityKeyPair
        deviceId = store.deviceId

        // make sure we have a contact entry for ourselves
        db.mutate { tx ->
            tx.get<Model.Contact>(Schema.PATH_ME) ?: tx.put(
                Schema.PATH_ME,
                Model.Contact.newBuilder().setId(identityKeyPair.publicKey.toString()).build()
            )
        }

        // on startup, register some pre keys
        registerPreKeys(numInitialPreKeysToRegister)

        // listen for outbound messages (including pulling any previously unprocessed outbound
        // messages)
        db.subscribe(object :
            Subscriber<Model.OutgoingShortMessage>(
                subscriberId,
                "${Schema.PATH_OUTBOUND}/"
            ) {
            override fun onUpdate(path: String, value: Model.OutgoingShortMessage) {
                encryptAndSend(value)
            }

            override fun onDelete(path: String) {
                // not interested
            }
        })
    }

    val db: DB get() = store.db

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
        identityKey: String,
        text: String?,
        oggVoice: ByteArray? = null
    ): Model.ShortMessageRecord {
        if (text == null && oggVoice == null) {
            throw IllegalArgumentException("Please specify either text or oggVoice")
        } else if (text != null && oggVoice != null) {
            throw IllegalArgumentException("Please specify either text or oggVoice but not both")
        }
        val shortMessageBuilder = Model.ShortMessage.newBuilder().setId(randomMessageId).setSent(
            nowUnixNano
        )
        if (text != null) {
            shortMessageBuilder.text = text
        } else {
            shortMessageBuilder.oggVoice = oggVoice?.byteString()
        }
        val msg = shortMessageBuilder.build()
        val outgoing = Model.OutgoingShortMessage.newBuilder().setIdentityKey(identityKey)
            .addRemainingRecipients(identityKey).setMessage(msg).build()
        val msgRecord =
            Model.ShortMessageRecord.newBuilder()
                .setSenderId(store.identityKeyPair.publicKey.toString()).setId(msg.id.base32)
                .setSent(msg.sent)
                .setMessage(msg.toByteString()).setDirection(Model.ShortMessageRecord.Direction.OUT)
                .setStatus(Model.ShortMessageRecord.DeliveryStatus.UNSENT).build()
        db.mutate { tx ->
            // save the message in a list of all messages
            tx.put(msgRecord.dbPath, msgRecord)
            // update the relevant contact
            val contact = updateDirectContactMetaData(tx, identityKey, msg.sent, msg.text)
            // save the message under the relevant contact messages
            tx.put(msgRecord.contactMessagePath(contact), msgRecord.dbPath)
            // enqueue the outgoing message in the db for sending (actual send happens in the
            // message processing loop)
            tx.put(msgRecord.outboundPath, outgoing)
        }
        return msgRecord
    }

    private fun updateDirectContactMetaData(
        tx: Transaction,
        identityKey: String,
        messageTime: Long = 0,
        messageText: String = ""
    ): Model.Contact {
        val contactPath = identityKey.directContactPath
        val contact = tx.get<Model.Contact>(contactPath)
            ?: throw IllegalArgumentException("unknown direct contact")
        if (messageTime <= contact.mostRecentMessageTime) {
            return contact
        }
        // delete existing index entry
        tx.delete(contact.timestampedIdxPath)
        // update the contact
        val updatedContact = contact.toBuilder().setMostRecentMessageTime(messageTime)
            .setMostRecentMessageText(messageText).build()
        tx.put(contactPath, updatedContact)
        // create a new index entry
        tx.put(updatedContact.timestampedIdxPath, contactPath)
        return updatedContact
    }

    // unregisters the current identity from tassis
    fun unregister() {
        executor.submit {
            withAuthenticatedClient(object : ClientCallback<AuthenticatedClient> {
                override fun onClient(client: AuthenticatedClient) {
                    client.unregister(object : Callback<Unit> {
                        override fun onSuccess(result: Unit) {
                            logger.debug("successfully unregistered")
                        }

                        override fun onError(err: Throwable) {
                            logger.error("failed to unregister: ${err.message}", err)
                        }

                    })
                }

                override fun onClientUnavailable(err: Throwable) {
                    logger.error("failed to unregister: ${err.message}", err)
                }
            })
        }.get()
    }

    private fun registerPreKeys(numPreKeys: Int, delayMillis: Long = 0) {
        schedule(delayMillis, TimeUnit.MILLISECONDS) {
            try {
                db.mutate {
                    val spk = store.nextSignedPreKey
                    val otpks = store.generatePreKeys(numPreKeys)
                    withAuthenticatedClient(object : ClientCallback<AuthenticatedClient> {
                        override fun onClient(client: AuthenticatedClient) {
                            client.register(
                                spk.serialize(),
                                otpks.map { it.serialize() },
                                object : Callback<Unit> {
                                    override fun onSuccess(result: Unit) {
                                        logger.debug("successfully registered pre keys")
                                    }

                                    override fun onError(err: Throwable) {
                                        logger.error(
                                            "failed to register pre keys: ${err.message}",
                                            err
                                        )
                                    }

                                })
                        }

                        override fun onClientUnavailable(err: Throwable) {
                            logger.error("failed to register pre keys: ${err.message}", err)
                        }
                    })
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

    private fun encryptAndSend(outgoingMessage: Model.OutgoingShortMessage) {
        val timeSinceFailure = nowUnixNano - outgoingMessage.lastFailed
        val delayNanos = (failedSendRetryDelayMillis.millisToNanos - timeSinceFailure)
        schedule(delayNanos.nanosToMillis, TimeUnit.MILLISECONDS) {
            withAnonymousClient(object : ClientCallback<AnonymousClient> {
                override fun onClient(client: AnonymousClient) {
                    val unsentRecipients = HashSet(outgoingMessage.remainingRecipientsList)
                    outgoingMessage.remainingRecipientsList.forEach { recipient ->
                        try {
                            encryptAndSendTo(client, outgoingMessage, recipient)
                            unsentRecipients.remove(recipient)
                        } catch (t: Throwable) {
                            logger.debug("error sending, will retry: ${t.message}")
                        }
                    }
//                    val successful = unsentRecipients.size == 0
//                    val permanentlyFailed =
//                        !successful && outgoingMessage.lastFailed > 0 && nowUnixNano - outgoingMessage.lastFailed > stopSendRetryAfterMillis.millisToNanos
//                    val deliveryStatus = if (successful) {
//                        Model.ShortMessageRecord.DeliveryStatus.SENT
//                    } else if (permanentlyFailed) {
//                        if (unsentRecipients.size < outgoingMessage.remainingRecipientsCount) {
//                            Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_FAILED
//                        } else {
//                            Model.ShortMessageRecord.DeliveryStatus.PARTIALLY_FAILED
//                        }
//                    } else {
//                        Model.ShortMessageRecord.DeliveryStatus.FAILING
//                    }
//                    val userMessage =
//                        outgoingMessage.message.outbound(
//                            store.identityKeyPair.publicKey.toString(),
//                            deliveryStatus
//                        )
//                    db.mutate { tx ->
//                        tx.put(userMessage.dbPath, userMessage)
//                        if (successful || permanentlyFailed) {
//                            tx.delete(userMessage.outboundPath)
//                        } else {
//                            val outbound =
//                                outgoingMessage.toBuilder().clearRemainingRecipients()
//                                    .addAllRemainingRecipients(unsentRecipients.toList())
//                                    .setLastFailed(nowUnixNano)
//                                    .build()
//                            tx.put(userMessage.outboundPath, outbound)
//                        }
//                    }
                }

                override fun onClientUnavailable(err: Throwable) {
                    // schedule to retry later
                    schedule(
                        failedSendRetryDelayMillis,
                        TimeUnit.MILLISECONDS,
                        { encryptAndSend(outgoingMessage) })
                }

            })

        }
    }

    private fun encryptAndSendTo(
        client: AnonymousClient,
        outgoingMessage: Model.OutgoingShortMessage,
        recipient: String
    ) {
        db.mutate { tx ->
            // mark message as SENDING
            val shortMessage =
                outgoingMessage.message.outbound(
                    store.identityKeyPair.publicKey.toString(),
                    Model.ShortMessageRecord.DeliveryStatus.SENDING
                )
            tx.put(shortMessage.dbPath, shortMessage)
            val recipientIdentityKey = ECPublicKey(recipient)
            val knownDeviceIds =
                store.getSubDeviceSessions(recipientIdentityKey.toString())
            if (knownDeviceIds.size > 0) {
                // we know device IDs, send to the ones we know
                // TODO: figure out how to handle future additions of recipient devices (maybe retrieve preKeys periodically?)
                doEncryptAndSendTo(client, outgoingMessage, recipientIdentityKey, knownDeviceIds)
            } else {
                // need to fetch pre keys and then we can send
                retrievePreKeys(client, outgoingMessage, recipientIdentityKey, knownDeviceIds)
            }
        }
    }

    private fun doEncryptAndSendTo(
        client: AnonymousClient,
        outgoingMessage: Model.OutgoingShortMessage,
        recipientIdentityKey: ECPublicKey,
        deviceIds: List<DeviceId>
    ) {
        deviceIds.forEach { deviceId ->
            val transferMsg =
                Model.TransferMessage.newBuilder().setShortMessage(outgoingMessage.message).build()
            // TODO: we (mostly Signal) use ByteArray everywhere, but Protocol Buffers wants byte strings
            // which have to be copied from the ByteArray. That results in a lot of extra copies,
            // it would  sure be nice to avoid that.
            val plainText = transferMsg.toByteArray()
            val paddedPlainText = Padding.padMessage(plainText)
            val to = SignalProtocolAddress(recipientIdentityKey, deviceId)
            val unidentifiedSenderMessage: ByteArray =
                cipher.encrypt(to, paddedPlainText)
            client.sendUnidentifiedSenderMessage(
                to,
                unidentifiedSenderMessage,
                object : Callback<Unit> {
                    override fun onSuccess(result: Unit) {
                        logger.debug("successfully sent message")
                        submit {
                            updateOutgoingStatus(outgoingMessage, recipientIdentityKey.toString())
                        }
                    }

                    override fun onError(err: Throwable) {
                        // TODO: mark permanent failure
                        logger.error("failed to send message: ${err.message}", err)
                    }
                })
        }
    }

    private fun retrievePreKeys(
        client: AnonymousClient,
        outgoingMessage: Model.OutgoingShortMessage,
        recipientIdentityKey: ECPublicKey,
        knownDeviceIds: List<DeviceId>
    ) {
        client.retrievePreKeys(
            recipientIdentityKey,
            knownDeviceIds,
            object : Callback<List<Messages.PreKey>> {
                override fun onSuccess(result: List<Messages.PreKey>) {
                    db.mutate { tx ->
                        result.forEach { preKey ->
                            val signedPreKey =
                                SignedPreKeyRecord(preKey.signedPreKey.toByteArray())
                            val oneTimePreKey =
                                PreKeyRecord(preKey.oneTimePreKey.toByteArray())
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
                    }
                    // submit the message for re-processing
                    submit { encryptAndSend(outgoingMessage) }
                }

                override fun onError(err: Throwable) {
                    logger.debug("error retrieving pre keys, try to re-process outoing message later: $err")
                    schedule(
                        failedSendRetryDelayMillis,
                        TimeUnit.MILLISECONDS,
                        { encryptAndSend(outgoingMessage) })
                }
            })
    }

    private fun updateOutgoingStatus(
        outgoingMessage: Model.OutgoingShortMessage,
        recipient: String
    ) {
        val remainingRecipients =
            HashSet(outgoingMessage.remainingRecipientsList)
        remainingRecipients.remove(recipient)
        val finished = remainingRecipients.size == 0
        val permanentlyFailed =
            !finished && outgoingMessage.lastFailed > 0 && nowUnixNano - outgoingMessage.lastFailed > stopSendRetryAfterMillis.millisToNanos
        val deliveryStatus = if (finished) {
            Model.ShortMessageRecord.DeliveryStatus.SENT
        } else if (permanentlyFailed) {
            if (remainingRecipients.size < outgoingMessage.remainingRecipientsCount) {
                Model.ShortMessageRecord.DeliveryStatus.COMPLETELY_FAILED
            } else {
                Model.ShortMessageRecord.DeliveryStatus.PARTIALLY_FAILED
            }
        } else {
            Model.ShortMessageRecord.DeliveryStatus.SENDING
        }
        val shortMessage =
            outgoingMessage.message.outbound(
                store.identityKeyPair.publicKey.toString(),
                deliveryStatus
            )
        db.mutate { tx ->
            tx.put(shortMessage.dbPath, shortMessage)
            if (finished || permanentlyFailed) {
                tx.delete(shortMessage.outboundPath)
            } else {
                val outbound =
                    outgoingMessage.toBuilder().clearRemainingRecipients()
                        .addAllRemainingRecipients(remainingRecipients.toList())
                        .setLastFailed(nowUnixNano)
                        .build()
                tx.put(shortMessage.outboundPath, outbound)
            }
        }
    }

    private fun decryptAndStore(inbound: InboundMessage) {
        submit {
            try {
                doDecryptAndStore(inbound)
            } catch (e: Exception) {
                logger.error("problem decrypting and storing message, dropping: ${e.message}")
                // TODO: maybe add this to a failed folder and/or a spam folder
            }
            inbound.ack()
        }
    }

    private fun doDecryptAndStore(inbound: InboundMessage) {
        db.mutate { tx ->
            val decryptionResult = cipher.decrypt(inbound.data.toByteArray())
            val plainText = Padding.stripMessagePadding(decryptionResult.paddedMessage)
            val transferMsg = Model.TransferMessage.parseFrom(plainText)
            val senderAddress = decryptionResult.senderAddress
            val senderId = senderAddress.identityKey.toString()
            if (!tx.contains(senderId.directContactPath)) {
                throw UnknownSenderException()
            }
            val msgRecord = transferMsg.shortMessage.inbound(senderId)
            val msg = Model.ShortMessage.parseFrom(msgRecord.message)
            // save the message record itself
            tx.put(msgRecord.dbPath, msgRecord)
            // update the Contact metadata
            val contact = updateDirectContactMetaData(tx, senderId, msg.sent, msg.text)
            // save a pointer to the message under the contact message path
            tx.put(msgRecord.contactMessagePath(contact), msgRecord.dbPath)
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

    /**
     * Callback interface for receiving clients
     */
    interface ClientCallback<T> {
        fun onClient(client: T)

        fun onClientUnavailable(err: Throwable)
    }

    private fun withAnonymousClient(cb: ClientCallback<AnonymousClient>) {
        anonymousClientLock.acquire()
        val existing = anonymousClient
        if (existing != null) {
            val client = existing
            anonymousClientLock.release()
            cb.onClient(client)
            return
        }

        val newClient = AnonymousClient(object : AnonymousClientDelegate {
            override fun onConnected(client: AnonymousClient) {
                logger.debug("successfully connected anonymous client to tassis")
                anonymousClient = client
                anonymousClientConnectFailures = 0
                anonymousClientLock.release()
                cb.onClient(client)
            }

            override fun onConnectError(err: Throwable) {
                anonymousClientConnectFailures++
                cb.onClientUnavailable(err)
            }

            override fun onClose(err: Throwable?) {
                if (err != null) {
                    logger.error("tassis client closed with error ${err}")
                } else {
                    logger.debug("tassis client closed normally")
                }
                anonymousClientLock.acquire()
                anonymousClient = null
                anonymousClientLock.release()
            }
        })

        if (anonymousClientConnectFailures > 0) {
            val redialDelay =
                (redialBackoffMillis * 2.0.pow(anonymousClientConnectFailures)).toLong()
            val actualRedialDelay =
                if (maxRedialDelayMillis < redialDelay) maxRedialDelayMillis else redialDelay
            logger.debug("due to $anonymousClientConnectFailures previous errors connecting anonymous client to tassis, will wait ${actualRedialDelay}ms before dialing again")
            Thread.sleep(actualRedialDelay)
        }

        logger.debug("attempting to connect anonymous client to tassis")
        transportFactory.connect(newClient)
    }

    private fun withAuthenticatedClient(cb: ClientCallback<AuthenticatedClient>) {
        authenticatedClientLock.acquire()
        val existing = authenticatedClient
        if (existing != null) {
            val client = existing
            authenticatedClientLock.release()
            cb.onClient(client)
            return
        }

        val newClient = AuthenticatedClient(identityKeyPair.publicKey,
            deviceId,
            object : AuthenticatedClientDelegate {
                override fun onConnected(client: AuthenticatedClient) {
                    logger.debug("successfully connected authenticated client to tassis")
                    authenticatedClient = client
                    authenticatedClientConnectFailures = 0
                    authenticatedClientLock.release()
                    cb.onClient(client)
                }

                override fun onConnectError(err: Throwable) {
                    authenticatedClientConnectFailures++
                    cb.onClientUnavailable(err)
                }

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
                    if (err != null) {
                        logger.error("tassis client closed with error ${err}")
                    } else {
                        logger.debug("tassis client closed normally")
                    }
                    authenticatedClientLock.acquire()
                    authenticatedClient = null
                    authenticatedClientLock.release()
                }
            })

        if (authenticatedClientConnectFailures > 0) {
            val redialDelay =
                (redialBackoffMillis * 2.0.pow(authenticatedClientConnectFailures)).toLong()
            val actualRedialDelay =
                if (maxRedialDelayMillis < redialDelay) maxRedialDelayMillis else redialDelay
            logger.debug("due to $authenticatedClientConnectFailures previous errors connecting authenticated client to tassis, will wait ${actualRedialDelay}ms before dialing again")
            Thread.sleep(actualRedialDelay)
        }

        logger.debug("attempting to connect authenticated client to tassis")
        transportFactory.connect(newClient)
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
        db.unsubscribe(subscriberId)
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
    get() = System.currentTimeMillis().millisToNanos

