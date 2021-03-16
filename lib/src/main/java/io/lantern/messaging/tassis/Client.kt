/*
 * This Kotlin source file was generated by the Gradle 'init' task.
 */
package io.lantern.messaging.tassis

import com.google.protobuf.ByteString
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.ECPublicKey
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

internal val logger = KotlinLogging.logger {}

/**
 * Callback interface for receiving asynchronous results.
 */
interface Callback<T> {
    fun onSuccess(result: T)

    fun onError(err: Throwable)
}

internal class BlockingCallback<T> : Callback<T> {
    val blockingResult = java9.util.concurrent.CompletableFuture<T>()

    override fun onSuccess(result: T) {
        blockingResult.complete(result)
    }

    override fun onError(err: Throwable) {
        fail(err)
    }

    fun await(): T = blockingResult.get()

    fun fail(err: Throwable) {
        blockingResult.completeExceptionally(err)
    }
}

/**
 * A handler for messages received from a transport (e.g. a websocket connection)
 */
interface MessageHandler {
    /**
     * Called whenever a new message arrives from the remote end
     */
    fun onMessage(data: ByteBuffer?)

    /**
     * Called when the transport has been closed to let the MessageHandler know that it should close
     * too. If the transport was closed due to an error condition, err will be populated.
     */
    fun onClose(err: Throwable? = null)
}

/**
 * A facility for sending messages (e.g. a websocket connection)
 */
interface Transport {
    fun send(data: ByteArray)

    fun close()
}

/**
 * A factory for Transports.
 */
interface TransportFactory {
    fun build(handler: MessageHandler, cb: Callback<Transport>)
}

/**
 * Represents an error from tassis
 */
data class TassisError(val name: String, val description: String) :
    RuntimeException("${name}: ${description}") {
    internal constructor(msg: Messages.Error) : this(msg.name, msg.description)
}


/**
 * Represents an inbound message from another user. Call ack() once the message has been durably
 * recorded so that it can be deleted server-side.
 */
class InboundMessage internal constructor(
    private val msg: Messages.Message,
    private val client: AuthenticatedClient
) {
    val data: ByteString
        get() = msg.inboundMessage

    fun ack() {
        try {
            client.send(
                Messages.Message.newBuilder().setSequence(msg.sequence)
                    .setAck(Messages.Ack.newBuilder().build()).build()
            )
        } catch (t: Throwable) {
            logger.debug("ack failed with error '${t.message}', this can happen if the client connection was closed before we could process the inbound message, in which case we might get the same message again later, which is okay")
        }
    }
}

/**
 * A delegate for receiving lifecycle callbacks from a Client connection.
 */
interface ClientDelegate {
    /**
     * Called when a client closes. If the client closed due to an exception, err will be populated
     * with the relevant exception.
     */
    fun onClose(err: Throwable? = null)
}

/**
 * A delegate for receiving asynchronous callbacks on an authenticated client connection.
 */
interface AuthenticatedClientDelegate : ClientDelegate {
    /**
     * Used to sign the response to an AuthChallenge
     */
    fun signLogin(loginBytes: ByteArray): ByteArray

    /**
     * Called when tassis notifies us that one-time pre keys are getting low.
     */
    fun onPreKeysLow(numPreKeysRequested: Int)

    /**
     * Called when we receive an inbound message from another user device via tassis.
     */
    fun onInboundMessage(msg: InboundMessage)
}

/**
 * Client provides a mechanism for talking to a tassis service.
 */
abstract class Client<D : ClientDelegate>(
    protected val delegate: D
) : MessageHandler {
    internal val transport = BlockingCallback<Transport>()
    private val msgSequence = AtomicInteger()
    protected val pending = ConcurrentHashMap<Int, Callback<Any?>>()

    internal fun send(msg: Messages.Message, callback: Callback<*>? = null) {
        if (callback != null) {
            pending[msg.sequence] = callback as Callback<Any?>
        }
        transport.await().send(msg.toByteArray())
    }

    protected fun nextMessage(): Messages.Message.Builder {
        return Messages.Message.newBuilder().setSequence(msgSequence.incrementAndGet())
    }

    override fun onMessage(data: ByteBuffer?) {
        val msg = Messages.Message.parseFrom(data)
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.ACK -> { pending[msg.sequence]?.onSuccess(null) }
            Messages.Message.PayloadCase.ERROR -> pending[msg.sequence]?.onError(
                TassisError(
                    msg.error!!.name,
                    msg.error!!.description
                )
            )
            else -> println("unknown payload type ${msg.payloadCase}")
        }
    }

    fun close() {
        transport.await().close()
    }

    override fun onClose(err: Throwable?) {
        pending.clear()
        delegate.onClose(err)
    }
}

/**
 * Authenticated client is a Client that has authenticated against the tassis cluster and can be
 * used for operations that require authentication.
 */
class AuthenticatedClient private constructor(
    private val identityKey: ECPublicKey,
    private val deviceId: DeviceId,
    delegate: AuthenticatedClientDelegate,
    private val connectCallback: Callback<AuthenticatedClient>
) : Client<AuthenticatedClientDelegate>(delegate) {
    fun register(signedPreKey: ByteArray, preKeys: List<ByteArray>) {
        val msg = nextMessage().setRegister(
            Messages.Register.newBuilder().setSignedPreKey(signedPreKey.byteString())
                .addAllOneTimePreKeys(preKeys.map { it.byteString() }).build()
        ).build()
        val cb = BlockingCallback<Messages.Ack>()
        send(msg, cb)
        cb.await()
    }

    fun unregister() {
        val msg = nextMessage().setUnregister(Messages.Unregister.newBuilder().build()).build()
        val cb = BlockingCallback<Messages.Ack>()
        send(msg, cb)
        cb.await()
    }

    override fun onMessage(data: ByteBuffer?) {
        val msg = Messages.Message.parseFrom(data)
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.AUTHCHALLENGE -> processAuth(msg.authChallenge)
            Messages.Message.PayloadCase.PREKEYS -> pending[msg.sequence]?.onSuccess(msg.preKeys)
            Messages.Message.PayloadCase.PREKEYSLOW -> delegate.onPreKeysLow(msg.preKeysLow.keysRequested)
            Messages.Message.PayloadCase.INBOUNDMESSAGE -> delegate.onInboundMessage(
                InboundMessage(
                    msg,
                    this
                )
            )
            else -> super.onMessage(data)
        }
    }

    private fun processAuth(challenge: Messages.AuthChallenge) {
        logger.debug("Processing auth")
        val login = Messages.Login.newBuilder()
            .setNonce(challenge.nonce)
            .setAddress(
                Messages.Address.newBuilder()
                    .setIdentityKey(identityKey.bytes.byteString())
                    .setDeviceId(deviceId.bytes.byteString())
            ).build()
        val loginBytes = login.toByteArray()
        logger.debug("signing auth")
        val signature = delegate.signLogin(loginBytes)
        val authResponse = Messages.Message.newBuilder().setAuthResponse(
            Messages.AuthResponse.newBuilder()
                .setLogin(loginBytes.byteString())
                .setSignature(signature.byteString())
        ).build()
        logger.debug("sending auth response")
        val cb = BlockingCallback<Messages.Ack>()
        send(authResponse, cb)
        logger.debug("sent auth response")
//        try {
//            cb.await()
//            logger.debug("auth successful")
//            connectCallback.onSuccess(this)
//        } catch (t: Throwable) {
//            logger.debug("auth failed")
//            connectCallback.onError(t)
//        }
        connectCallback.onSuccess(this)
    }

    companion object {
        /**
         * Connects an AuthenticatedClient using the given transportFactory.
         */
        fun connect(
            transportFactory: TransportFactory,
            identityKey: ECPublicKey,
            deviceId: DeviceId,
            delegate: AuthenticatedClientDelegate,
        ): AuthenticatedClient {
            val cb = BlockingCallback<AuthenticatedClient>()
            val client = AuthenticatedClient(identityKey, deviceId, delegate, cb)
            transportFactory.build(client, client.transport)
            return cb.await()
        }
    }
}

/**
 * Anonymous client is a client that does not authenticate against the tassis service. It is used
 * for anonymous operations like requesting pre keys and sending sealed sender messages.
 */
class AnonymousClient private constructor(
    delegate: ClientDelegate,
    private val connectCallback: Callback<AnonymousClient>
) : Client<ClientDelegate>(delegate) {
    fun retrievePreKeys(
        identityKey: ECPublicKey,
        knownDeviceIds: List<DeviceId>
    ): List<Messages.PreKey> {
        val requestPreKeys = Messages.RequestPreKeys.newBuilder()
            .setIdentityKey(identityKey.bytes.byteString())
        knownDeviceIds.forEach { requestPreKeys.addKnownDeviceIds(it.bytes.byteString()) }
        val msg = nextMessage().setRequestPreKeys(requestPreKeys.build()).build()
        val cb = BlockingCallback<Messages.PreKeys>()
        send(msg, cb)
        return cb.await().preKeysList
    }

    fun sendUnidentifiedSenderMessage(
        to: SignalProtocolAddress,
        unidentifiedSenderMessage: ByteArray
    ) {
        val msg = nextMessage().setOutboundMessage(
            Messages.OutboundMessage.newBuilder().setTo(
                Messages.Address.newBuilder()
                    .setIdentityKey(to.identityKey.bytes.byteString())
                    .setDeviceId(to.deviceId.bytes.byteString())
            ).setUnidentifiedSenderMessage(unidentifiedSenderMessage.byteString())
        ).build()
        val cb = BlockingCallback<Messages.Ack>()
        send(msg, cb)
        cb.await()
    }

    override fun onMessage(data: ByteBuffer?) {
        val msg = Messages.Message.parseFrom(data)
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.AUTHCHALLENGE -> connectCallback.onSuccess(this)
            Messages.Message.PayloadCase.PREKEYS -> pending[msg.sequence]?.onSuccess(msg.preKeys)
            else -> super.onMessage(data)
        }
    }

    companion object {
        /**
         * Connects an AnonymousClient using the given transportFactory.
         */
        fun connect(
            transportFactory: TransportFactory,
            delegate: ClientDelegate
        ): AnonymousClient {
            val cb = BlockingCallback<AnonymousClient>()
            val client = AnonymousClient(delegate, cb)
            transportFactory.build(client, client.transport)
            return cb.await()
        }
    }
}

fun ByteArray.message(): Messages.Message {
    return Messages.Message.parseFrom(this)
}

fun ByteArray.byteString(): ByteString {
    return ByteString.copyFrom(this)
}