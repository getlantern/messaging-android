package io.lantern.messaging.tassis

import com.google.protobuf.ByteString
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.ECPublicKey
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

internal val logger = KotlinLogging.logger {}

interface Dialer {
    suspend fun dial(scope: CoroutineScope): ClientConnection
}

interface ClientConnection : Closeable {
    abstract val outbound: SendChannel<ByteArray>
    abstract val inbound: ReceiveChannel<ByteArray>
}

class Client(
    internal val identityKey: ECPublicKey,
    internal val deviceId: DeviceId,
    anonymousConn: ClientConnection,
    authenticatedConn: ClientConnection,
    internal val scope: CoroutineScope,
    internal val anonymousOutboundBufferDepth: Int = 100,
    internal val authenticatedOutboundBufferDepth: Int = 10,
    authenticatedInboundBufferDepth: Int = 100,
    internal val signLogin: (ByteArray) -> ByteArray
) {
    internal val inbound = Channel<InboundMessage>(authenticatedInboundBufferDepth)
    internal val preKeysLow = Channel<Int>(1)

    private val anonymousHandler = AnonymousConnectionHandler(this, anonymousConn)
    private val authenticatedHandler =
        AuthenticatedConnectionHandler(this, authenticatedConn)
    private val msgSequence = AtomicInteger()

    init {
        scope.launch {
            anonymousHandler.processInbound()
        }
        scope.launch {
            authenticatedHandler.processInbound()
        }
    }

    suspend fun retrievePreKeys(
        identityKey: ECPublicKey,
        knownDeviceIds: List<DeviceId>
    ): List<Messages.PreKey> {
        val requestPreKeys = Messages.RequestPreKeys.newBuilder()
            .setIdentityKey(identityKey.bytes.byteString())
        knownDeviceIds.forEach { requestPreKeys.addKnownDeviceIds(it.bytes.byteString()) }
        val msg = nextMessage().setRequestPreKeys(requestPreKeys.build()).build()
        val cb = Callback<Messages.PreKeys>()
        anonymousHandler.send(msg, cb)
        return select {
            cb.result.onReceive { it.preKeysList }
            cb.error.onReceive { throw it }
        }
    }

    suspend fun registerPreKeys(signedPreKey: ByteArray, preKeys: List<ByteArray>) {
        val msg = nextMessage().setRegister(
            Messages.Register.newBuilder().setSignedPreKey(signedPreKey.byteString())
                .addAllOneTimePreKeys(preKeys.map { it.byteString() }).build()
        )

        val cb = Callback<Messages.Ack>()
        authenticatedHandler.send(msg.build(), cb)
        // return immediately and handle error later
        scope.launch {
            select<Unit> {
                cb.result.onReceive {
                    // okay
                }
                cb.error.onReceive {
                    throw it
                }
            }
        }
    }

    suspend fun sendUnidentifiedSenderMessage(
        to: SignalProtocolAddress,
        unidentifiedSenderMessage: ByteArray
    ) {
        val msg = nextMessage().setOutboundMessage(
            Messages.OutboundMessage.newBuilder().setTo(
                Messages.Address.newBuilder()
                    .setIdentityKey(to.identityKey.bytes.byteString())
                    .setDeviceId(to.deviceId.bytes.byteString())
            ).setUnidentifiedSenderMessage(unidentifiedSenderMessage.byteString())
        )
        val cb = Callback<Messages.Ack>()
        anonymousHandler.send(msg.build(), cb)
        select<Unit> {
            cb.result.onReceive {
                // okay
            }
            cb.error.onReceive {
                throw it
            }
        }
    }

    protected fun nextMessage(): Messages.Message.Builder {
        return Messages.Message.newBuilder().setSequence(msgSequence.incrementAndGet())
    }
}

/**
 * Represents an error from tassis
 */
data class TassisError(val name: String, val description: String) :
    Exception("${name}: ${description}") {
    internal constructor(msg: Messages.Error) : this(msg.name, msg.description)
}

/**
 * Callback interface for receiving asynchronous results.
 */
internal class Callback<T>(
    val result: Channel<T> = Channel(1),
    val error: Channel<TassisError> = Channel(1)
) {
    suspend fun onSuccess(value: T) {
        result.send(value)
    }

    suspend fun onError(msg: Messages.Error) {
        error.send(TassisError(msg))
    }
}

internal abstract class ConnectionHandler(
    protected val client: Client,
    protected val conn: ClientConnection,
    outboundBufferDepth: Int
) {
    protected val pending = ConcurrentHashMap<Int, Callback<Any?>>()
    internal val outbound = Channel<Messages.Message>(outboundBufferDepth)

    internal suspend fun send(
        msg: Messages.Message,
        callback: Callback<*>? = null
    ) {
        if (callback != null) {
            pending[msg.sequence] = callback as Callback<Any?>
        }
        outbound.send(msg)
    }

    open suspend fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.ACK -> pending.remove(msg.sequence)?.onSuccess(null)
            Messages.Message.PayloadCase.ERROR -> pending.remove(msg.sequence)?.onError(msg.error)
            else -> logger.error("unknown payload type ${msg.payloadCase}")
        }
    }

    suspend fun processInbound() {
        val msg = conn.inbound.receive().message()
        processAuth(msg.authChallenge)
        client.scope.launch { processOutbound() }
        for (msgBytes in conn.inbound) {
            onMessage(msgBytes.message())
        }
    }

    suspend fun processOutbound() {
        for (msg in outbound) {
            conn.outbound.send(msg.toByteArray())
        }
    }

    abstract suspend fun processAuth(challenge: Messages.AuthChallenge)
}

internal class AnonymousConnectionHandler constructor(client: Client, conn: ClientConnection) :
    ConnectionHandler(client, conn, client.anonymousOutboundBufferDepth) {
    override suspend fun processAuth(challenge: Messages.AuthChallenge) {
        // ignore auth challenges on anonymous connections
    }

    override suspend fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.PREKEYS -> pending.remove(msg.sequence)
                ?.onSuccess(msg.preKeys)
            else -> return super.onMessage(msg)
        }
    }
}

internal class AuthenticatedConnectionHandler(
    client: Client, conn: ClientConnection
) : ConnectionHandler(client, conn, client.authenticatedOutboundBufferDepth) {
    override suspend fun processAuth(challenge: Messages.AuthChallenge) {
        val login = Messages.Login.newBuilder()
            .setNonce(challenge.nonce)
            .setAddress(
                Messages.Address.newBuilder()
                    .setIdentityKey(client.identityKey.bytes.byteString())
                    .setDeviceId(client.deviceId.bytes.byteString())
            ).build()
        val loginBytes = login.toByteArray()
        val signature = client.signLogin(loginBytes)
        val authResponse = Messages.Message.newBuilder().setAuthResponse(
            Messages.AuthResponse.newBuilder()
                .setLogin(loginBytes.byteString())
                .setSignature(signature.byteString())
        ).build()
        conn.outbound.send(authResponse.toByteArray())
        logger.debug("sent auth response")
        val inbound = conn.inbound
        val received = inbound.receive()
        val result = received.message()
        if (result.hasAck()) {
            // we're logged in!
        } else {
            throw Exception("Unable to log in: ${result.error.name}")
        }
    }

    override suspend fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.PREKEYSLOW -> {
                client.preKeysLow.send(msg.preKeysLow.keysRequested)
            }
            Messages.Message.PayloadCase.INBOUNDMESSAGE -> client.inbound.send(
                InboundMessage(
                    msg,
                    this@AuthenticatedConnectionHandler
                )
            )
            else -> super.onMessage(msg)
        }
    }
}

/**
 * Represents an inbound message from another user. Call ack() once the message has been durably
 * recorded so that it can be deleted server-side.
 */
class InboundMessage internal constructor(
    private val msg: Messages.Message,
    private val handler: AuthenticatedConnectionHandler
) {
    val data: ByteString
        get() = msg.inboundMessage

    suspend fun ack() {
        handler.send(
            Messages.Message.newBuilder().setSequence(msg.sequence).setAck(
                Messages.Ack.newBuilder().build()
            ).build()
        )
    }
}

fun ByteArray.message(): Messages.Message {
    return Messages.Message.parseFrom(this)
}

fun ByteArray.byteString(): ByteString {
    return ByteString.copyFrom(this)
}