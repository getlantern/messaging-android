package io.lantern.messaging

import com.google.protobuf.ByteString
import io.lantern.db.DB
import io.lantern.db.Raw
import io.lantern.db.RawSubscriber
import io.lantern.db.Transaction
import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.*
import io.lantern.messaging.time.millisToNanos
import io.lantern.messaging.time.minutesToMillis
import io.lantern.messaging.time.secondsToMillis
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.ecc.ECKeyPair
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
    internal val name: String = "messaging",
) : Closeable {
    internal val logger = KotlinLogging.logger(name)

    // All processing that involves crypto operations happens on this executor to keep the
    // SignalProtocolStore in a consistent state
    private val cryptoWorker =
        CryptoWorker(this, failedSendRetryDelayMillis, stopSendRetryAfterMillis)

    private val anonymousClientLock = Semaphore(1)
    private var anonymousClientConnectFailures = 0
    private var anonymousClient: AnonymousClient? = null
    private val anonymousClientExecutor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${name}-anonymous-client-executor")
    }

    private val authenticatedClientLock = Semaphore(1)
    private var authenticatedClientConnectFailures = 0
    private var authenticatedClient: AuthenticatedClient? = null
    private val authenticatedClientExecutor = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "${name}-authenticated-client-executor")
    }

    internal val identityKeyPair: ECKeyPair
    private val deviceId: DeviceId

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
        // this also has the welcome side effect of starting an authenticated client, which we need
        // in order to receive messages
        cryptoWorker.registerPreKeys(numInitialPreKeysToRegister)

        // listen for outbound messages (including pulling any previously unprocessed outbound
        // messages)
        db.subscribe(object :
            RawSubscriber<Model.OutgoingShortMessage>(
                subscriberId,
                "${Schema.PATH_OUTBOUND}/"
            ) {
            override fun onUpdate(path: String, value: Raw<Model.OutgoingShortMessage>) {
                cryptoWorker.processOutgoing(path)
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
        val outgoing =
            Model.OutgoingShortMessage.newBuilder().setIdentityKey(identityKey).setMessage(msg)
                .build()
        val msgRecord =
            Model.ShortMessageRecord.newBuilder()
                .setSenderId(store.identityKeyPair.publicKey.toString()).setId(msg.id.base32)
                .setSent(msg.sent)
                .setMessage(msg.toByteString()).setDirection(Model.ShortMessageRecord.Direction.OUT)
                .setStatus(Model.ShortMessageRecord.DeliveryStatus.SENDING).build()
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

    internal fun updateDirectContactMetaData(
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
    }

    /**
     * Callback interface for receiving clients
     */
    interface ClientCallback<T> {
        fun onClient(client: T)

        fun onClientUnavailable(err: Throwable)
    }

    internal fun withAnonymousClient(cb: ClientCallback<AnonymousClient>) {
        anonymousClientLock.acquire()
        val existing = anonymousClient
        if (existing != null) {
            val client = existing
            anonymousClientLock.release()
            cb.onClient(client)
            return
        }

        logger.debug("building new anonymous client")
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
                    logger.error("anonymous tassis client closed with error ${err}")
                } else {
                    logger.debug("anonymous tassis client closed normally")
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

    internal fun withAuthenticatedClient(cb: ClientCallback<AuthenticatedClient>) {
        authenticatedClientLock.acquire()
        val existing = authenticatedClient
        if (existing != null) {
            val client = existing
            authenticatedClientLock.release()
            cb.onClient(client)
            return
        }

        logger.debug("building new authenticated client")
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
                    cryptoWorker.registerPreKeys(numPreKeysRequested)
                }

                override fun onInboundMessage(msg: InboundMessage) {
                    cryptoWorker.decryptAndStore(msg)
                }

                override fun onClose(err: Throwable?) {
                    if (err != null) {
                        logger.error("authenticated tassis client closed with error ${err}")
                    } else {
                        logger.debug("authenticated tassis client closed normally")
                    }
                    authenticatedClientLock.acquire()
                    authenticatedClient = null
                    authenticatedClientLock.release()
                    // schedule a new no-op with the authenticated client to make sure we're connected
                    // (this is needed to continue receiving messages)
                    cryptoWorker.submit {
                        withAuthenticatedClient(object : ClientCallback<AuthenticatedClient> {
                            override fun onClient(client: AuthenticatedClient) {
                                // no-op
                            }

                            override fun onClientUnavailable(err: Throwable) {
                                // no-op
                            }
                        })
                    }
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
        cryptoWorker.executor.shutdownNow()
        synchronized(this) {
            anonymousClient?.close()
            anonymousClient = null
            authenticatedClient?.close()
            authenticatedClient = null
        }
        cryptoWorker.executor.awaitTermination(10, TimeUnit.SECONDS)
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

