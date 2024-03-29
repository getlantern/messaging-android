package io.lantern.messaging.tassis

import com.google.protobuf.ByteString
import io.lantern.messaging.conversions.byteString
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.ECPublicKey

internal val logger = KotlinLogging.logger {}

class CloseTimedOutException : Exception("Close timed out")

/**
 * Callback interface for receiving asynchronous results.
 */
interface Callback<T> {
    fun onSuccess(result: T)

    fun onError(err: Throwable)
}

internal class AckCallback(private val cb: Callback<Unit>) : Callback<Messages.Ack> {
    override fun onSuccess(result: Messages.Ack) {
        try {
            cb.onSuccess(Unit)
        } catch (t: Throwable) {
            logger.error("error on client callback: $t.message", t)
        }
    }

    override fun onError(err: Throwable) {
        cb.onError(err)
    }
}

/**
 * A handler for messages received from a transport (e.g. a websocket connection)
 */
interface MessageHandler {
    /**
     * A place for the Transport to register itself
     */
    fun setTransport(transport: Transport)

    /**
     * Called if there's an error connecting to or communicating with the Transport
     */
    fun onFailure(err: Throwable)

    /**
     * Called whenever a new message arrives from the remote end
     */
    fun onMessage(data: ByteArray?)

    /**
     * Called when the transport has been closed to let the MessageHandler know that it should close
     * too.
     */
    fun onClose()
}

/**
 * A facility for sending messages (e.g. a websocket connection)
 */
interface Transport {
    fun send(data: ByteArray)

    /**
     * Immediately closes the transport
     */
    fun cancel()

    /**
     * Initiates a graceful close of the transport
     */
    fun close()
}

/**
 * A factory for Transports.
 */
interface TransportFactory {
    fun connect(handler: MessageHandler)
}

/**
 * Represents an error from tassis
 */
data class TassisError(val name: String, val description: String) :
    RuntimeException("$name: $description")

/**
 * Represents an inbound message from another user. Call ack() once the message has been durably
 * recorded so that it can be deleted server-side.
 */
class InboundMessage internal constructor(
    private val msg: Messages.Message,
    private val client: AuthenticatedClient
) {
    val data: ByteString
        get() = msg.inboundMessage.unidentifiedSenderMessage

    fun ack() {
        try {
            client.send(
                Messages.Message.newBuilder().setSequence(msg.sequence)
                    .setAck(Messages.Ack.newBuilder().build()).build()
            )
        } catch (t: Throwable) {
            logger.trace("ack failed with error '${t.message}', this can happen if the client connection was closed before we could process the inbound message, in which case we might get the same message again later, which is okay") // ktlint-disable max-line-length
        }
    }
}

/**
 * A delegate for receiving lifecycle callbacks from a Client connection.
 */
interface ClientDelegate {
    /**
     * Called if there's an error connecting the Client
     */
    fun onConnectError(err: Throwable)

    /**
     * Called whenever we get a new Configuration from the server
     */
    fun onConfigUpdate(cfg: Messages.Configuration)

    /**
     * Called when a client closes. If the client closed due to an exception, err will be populated
     * with the relevant exception. If the client closed while we were still in the process of
     * connecting, the delegate will only receive a call to onConnectError.
     */
    fun onClose(err: Throwable? = null)
}

/**
 * A delegate for receiving asynchronous callbacks on an authenticated client connection.
 */
interface AuthenticatedClientDelegate : ClientDelegate {
    /**
     * Called once the client has been connected
     */
    fun onConnected(client: AuthenticatedClient, number: Messages.ChatNumber)

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

interface AnonymousClientDelegate : ClientDelegate {
    /**
     * Called once the client has been connected
     */
    fun onConnected(client: AnonymousClient)
}

/**
 * Client provides a mechanism for talking to a tassis service.
 */
abstract class Client<D : ClientDelegate>(
    protected val delegate: D,
    private val roundTripTimeoutMillis: Long
) : MessageHandler {
    private var transport: Transport? = null
    private val msgSequence = AtomicInteger(1)
    protected val pending = ConcurrentHashMap<Int, Callback<Any?>>()
    protected val isConnecting = AtomicBoolean(true)
    private val closed = AtomicBoolean()
    private val timeoutChecker = Executors.newSingleThreadScheduledExecutor {
        Thread(it, "client-timeout-checker")
    }

    override fun setTransport(transport: Transport) {
        this.transport = transport
    }

    internal fun send(msg: Messages.Message, callback: Callback<*>? = null) {
        val msgSequence = msg.sequence
        if (callback != null) {
            pending[msgSequence] = callback as Callback<Any?>
        }
        transport?.send(msg.toByteArray()) ?: throw Exception("Attempted to send before connected")
        if (callback != null) {
            timeoutChecker.schedule(
                {
                    pending.remove(msgSequence)?.let {
                        // if any request times out, consider the whole connection bad and just close it
                        val err = TimeoutException("request timed out")
                        it.onError(err)
                        close()
                    }
                },
                roundTripTimeoutMillis, TimeUnit.MILLISECONDS
            )
        }
    }

    protected fun nextMessage(): Messages.Message.Builder {
        return Messages.Message.newBuilder().setSequence(msgSequence.incrementAndGet())
    }

    override fun onMessage(data: ByteArray?) {
        val msg = Messages.Message.parseFrom(data)
        onMessage(msg)
    }

    protected fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.ACK ->
                pending.remove(msg.sequence)?.onSuccess(msg.ack)
            Messages.Message.PayloadCase.ERROR -> pending.remove(msg.sequence)?.onError(
                TassisError(
                    msg.error!!.name,
                    msg.error!!.description
                )
            )
            Messages.Message.PayloadCase.CONFIGURATION -> delegate.onConfigUpdate(msg.configuration)
            Messages.Message.PayloadCase.CHATNUMBER ->
                pending.remove(msg.sequence)?.onSuccess(msg.chatNumber)
            else -> logger.debug("unknown payload type ${msg.payloadCase}")
        }
    }

    /**
     * Initiates a graceful close of this client
     */
    fun close() {
        transport?.close()
        timeoutChecker.schedule(
            {
                if (closed.compareAndSet(false, true)) {
                    logger.debug("transport failed to close in time, cancel")
                    transport?.cancel()
                    doClose(CloseTimedOutException())
                }
            },
            roundTripTimeoutMillis, TimeUnit.MILLISECONDS
        )
    }

    override fun onFailure(err: Throwable) {
        if (!closed.compareAndSet(false, true)) {
            // already closed
            return
        }
        doClose(err)
    }

    override fun onClose() {
        if (!closed.compareAndSet(false, true)) {
            // already closed
            return
        }
        doClose()
    }

    private fun doClose(err: Throwable? = null) {
        err?.let { logger.debug("client closed with error: ${it.message}") }
        timeoutChecker.shutdown() // TODO: move timeoutChecker to a companion object
        val finalErr = err ?: ClientClosedException()
        if (isConnecting.compareAndSet(true, false)) {
            logger.debug("client closed while still connecting, treat like connect error")
            delegate.onConnectError(finalErr)
        } else {
            delegate.onClose(err)
        }
        pending.values.forEach {
            it.onError(finalErr)
        }
        pending.clear()
    }
}

/**
 * Authenticated client is a Client that has authenticated against the tassis cluster and can be
 * used for operations that require authentication.
 */
class AuthenticatedClient(
    private val identityKey: ECPublicKey,
    private val deviceId: DeviceId,
    delegate: AuthenticatedClientDelegate,
    roundTripTimeoutMillis: Long
) : Client<AuthenticatedClientDelegate>(delegate, roundTripTimeoutMillis) {
    fun register(signedPreKey: ByteArray, preKeys: List<ByteArray>, cb: Callback<Unit>) {
        val msg = nextMessage().setRegister(
            Messages.Register.newBuilder().setSignedPreKey(signedPreKey.byteString())
                .addAllOneTimePreKeys(preKeys.map { it.byteString() }).build()
        ).build()
        send(msg, AckCallback(cb))
    }

    fun unregister(cb: Callback<Unit>) {
        val msg = nextMessage().setUnregister(Messages.Unregister.newBuilder().build()).build()
        send(msg, AckCallback(cb))
    }

    override fun onMessage(data: ByteArray?) {
        val msg = Messages.Message.parseFrom(data)
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.AUTHCHALLENGE -> processAuth(msg.authChallenge)
            Messages.Message.PayloadCase.PREKEYS -> pending.remove(msg.sequence)
                ?.onSuccess(msg.preKeys)
            Messages.Message.PayloadCase.PREKEYSLOW ->
                delegate.onPreKeysLow(msg.preKeysLow.keysRequested)
            Messages.Message.PayloadCase.INBOUNDMESSAGE -> delegate.onInboundMessage(
                InboundMessage(
                    msg,
                    this
                )
            )
            else -> super.onMessage(msg)
        }
    }

    private fun processAuth(challenge: Messages.AuthChallenge) {
        try {
            val login = Messages.Login.newBuilder()
                .setNonce(challenge.nonce)
                .setAddress(
                    Messages.Address.newBuilder()
                        .setIdentityKey(identityKey.bytes.byteString())
                        .setDeviceId(deviceId.bytes.byteString())
                ).build()
            val loginBytes = login.toByteArray()
            val signature = delegate.signLogin(loginBytes)
            val authResponse = Messages.Message.newBuilder().setAuthResponse(
                Messages.AuthResponse.newBuilder()
                    .setLogin(loginBytes.byteString())
                    .setSignature(signature.byteString())
            ).build()
            logger.debug("sending login")
            send(
                authResponse,
                object : Callback<Messages.ChatNumber> {
                    override fun onSuccess(result: Messages.ChatNumber) {
                        logger.debug("successfully logged in, my number is ${result.number}")
                        if (isConnecting.compareAndSet(true, false)) {
                            delegate.onConnected(this@AuthenticatedClient, result)
                        }
                    }

                    override fun onError(err: Throwable) {
                        logger.debug("error during login ${err.message}")
                        this@AuthenticatedClient.onFailure(err)
                    }
                }
            )
        } catch (err: Throwable) {
            onFailure(err)
        }
    }
}

/**
 * Anonymous client is a client that does not authenticate against the tassis service. It is used
 * for anonymous operations like requesting pre keys and sending sealed sender messages.
 */
class AnonymousClient(
    delegate: AnonymousClientDelegate,
    roundTripTimeoutMillis: Long
) :
    Client<AnonymousClientDelegate>(delegate, roundTripTimeoutMillis) {
    fun requestPreKeys(
        identityKey: ECPublicKey,
        knownDeviceIds: List<DeviceId>,
        cb: Callback<List<Messages.PreKey>>
    ) {
        val requestPreKeys = Messages.RequestPreKeys.newBuilder()
            .setIdentityKey(identityKey.bytes.byteString())
        knownDeviceIds.forEach { requestPreKeys.addKnownDeviceIds(it.bytes.byteString()) }
        val msg = nextMessage().setRequestPreKeys(requestPreKeys.build()).build()
        send(
            msg,
            object : Callback<Messages.PreKeys> {
                override fun onSuccess(result: Messages.PreKeys) {
                    try {
                        cb.onSuccess(result.preKeysList)
                    } catch (t: Throwable) {
                        logger.error("error after receiving pre keys: $t.message", t)
                    }
                }

                override fun onError(err: Throwable) {
                    cb.onError(err)
                }
            }
        )
    }

    fun findChatNumberByShortNumber(
        shortNumber: String,
        cb: Callback<Messages.ChatNumber>
    ) {
        val request = Messages.FindChatNumberByShortNumber.newBuilder()
            .setShortNumber(shortNumber).build()
        val msg = nextMessage().setFindChatNumberByShortNumber(request).build()
        send(
            msg,
            object : Callback<Messages.ChatNumber> {
                override fun onSuccess(result: Messages.ChatNumber) {
                    try {
                        cb.onSuccess(result)
                    } catch (t: Throwable) {
                        logger.error("error after finding chat number by short number: $t.message", t) // ktlint-disable max-line-length
                    }
                }

                override fun onError(err: Throwable) {
                    cb.onError(err)
                }
            }
        )
    }

    fun findChatNumberByIdentityKey(
        identityKey: ECPublicKey,
        cb: Callback<Messages.ChatNumber>
    ) {
        val request = Messages.FindChatNumberByIdentityKey.newBuilder()
            .setIdentityKey(identityKey.bytes.byteString()).build()
        val msg = nextMessage().setFindChatNumberByIdentityKey(request).build()
        send(
            msg,
            object : Callback<Messages.ChatNumber> {
                override fun onSuccess(result: Messages.ChatNumber) {
                    try {
                        cb.onSuccess(result)
                    } catch (t: Throwable) {
                        logger.error("error after finding chat number by identity key: $t.message", t) // ktlint-disable max-line-length
                    }
                }

                override fun onError(err: Throwable) {
                    cb.onError(err)
                }
            }
        )
    }

    fun sendUnidentifiedSenderMessage(
        to: SignalProtocolAddress,
        unidentifiedSenderMessage: ByteArray,
        cb: Callback<Unit>
    ) {
        val msg = nextMessage().setOutboundMessage(
            Messages.OutboundMessage.newBuilder().setTo(
                Messages.Address.newBuilder()
                    .setIdentityKey(to.identityKey.bytes.byteString())
                    .setDeviceId(to.deviceId.bytes.byteString())
            ).setUnidentifiedSenderMessage(unidentifiedSenderMessage.byteString())
        ).build()
        send(msg, AckCallback(cb))
    }

    fun requestUploadAuthorizations(
        numRequested: Int,
        cb: Callback<List<Messages.UploadAuthorization>>
    ) {
        val requestUploadAuthorizations = Messages.RequestUploadAuthorizations.newBuilder()
            .setNumRequested(numRequested).build()
        val msg = nextMessage().setRequestUploadAuthorizations(requestUploadAuthorizations).build()
        send(
            msg,
            object : Callback<Messages.UploadAuthorizations> {
                override fun onSuccess(result: Messages.UploadAuthorizations) {
                    try {
                        cb.onSuccess(result.authorizationsList)
                    } catch (t: Throwable) {
                        logger.error("error after receiving upload authorizations: $t.message", t)
                    }
                }

                override fun onError(err: Throwable) {
                    cb.onError(err)
                }
            }
        )
    }

    override fun onMessage(data: ByteArray?) {
        val msg = Messages.Message.parseFrom(data)
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.AUTHCHALLENGE -> {
                if (isConnecting.compareAndSet(true, false)) {
                    delegate.onConnected(this)
                }
            }
            Messages.Message.PayloadCase.PREKEYS -> pending.remove(msg.sequence)
                ?.onSuccess(msg.preKeys)
            Messages.Message.PayloadCase.UPLOADAUTHORIZATIONS -> pending.remove(msg.sequence)
                ?.onSuccess(msg.uploadAuthorizations)
            else -> super.onMessage(msg)
        }
    }
}

fun ByteArray.message(): Messages.Message {
    return Messages.Message.parseFrom(this)
}

class ClientClosedException : Exception("Client closed")
