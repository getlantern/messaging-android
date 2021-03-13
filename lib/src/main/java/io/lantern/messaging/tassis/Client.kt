package io.lantern.messaging.tassis

import com.google.protobuf.ByteString
import io.lantern.messaging.store.MessagingStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import mu.KotlinLogging
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

interface Dialer {
    suspend fun dial(scope: CoroutineScope): ClientConnection
}

interface ClientConnection {
    val outbound: SendChannel<ByteArray>
    val inbound: ReceiveChannel<ByteArray>
    val errors: ReceiveChannel<Throwable>

    fun close()
}

@ExperimentalTime
class Client(
    internal val store: MessagingStore,
    private val dialer: Dialer,
    internal val scope: CoroutineScope,
    private val redialBackoff: Duration = 50.milliseconds,
    private val maxRedialDelay: Duration = 15.seconds,
    anonymousOutboundBufferDepth: Int = 100,
    authenticatedOutboundBufferDepth: Int = 10,
    authenticatedInboundBufferDept: Int = 100,
) {
    private val anonymousHandler = AnonymousConnectionHandler(this, anonymousOutboundBufferDepth)
    private val authenticatedHandler =
        AuthenticatedConnectionHandler(
            this,
            authenticatedOutboundBufferDepth,
            authenticatedInboundBufferDept
        )
    private val msgSequence = AtomicInteger()

    init {
        scope.launch {
            anonymousHandler.processInbound()
        }
        scope.launch {
            authenticatedHandler.processInbound()
        }
    }

    suspend fun registerPreKeys(signedPreKey: SignedPreKeyRecord, preKeys: List<PreKeyRecord>) {
        val msg = nextMessage().setRegister(
            Messages.Register.newBuilder().setSignedPreKey(signedPreKey.serialize().byteString())
                .addAllOneTimePreKeys(preKeys.map { it.serialize().byteString() }).build()
        )

        val cb = Callback<Messages.Ack>()
        authenticatedHandler.send(msg.build(), cb)
        select<Unit> {
            cb.result.onReceive {
                // okay
            }
            cb.error.onReceive {
                throw it
            }
        }
    }

    suspend fun sendUserMessage(to: SignalProtocolAddress, unidentifiedSenderMessage: ByteArray) {
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

    internal suspend fun dial(): ClientConnection {
        var failures = 0
        while (true) {
            try {
                return dialer.dial(scope)
            } catch (e: Exception) {
                val redialDelay = redialBackoff * 2.0.pow(failures)
                val actualRedialDelay =
                    if (maxRedialDelay < redialDelay) maxRedialDelay else redialDelay
                delay(actualRedialDelay.toLongMilliseconds())
                failures++
            }
        }
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

@ExperimentalTime
internal abstract class ConnectionHandler(protected val client: Client, outboundBufferDepth: Int) {
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
        val conn = client.dial()
        val msg = conn.inbound.receive().message()
        processAuth(conn, msg.authChallenge)
        client.scope.launch { processOutbound(conn) }
        for (msgBytes in conn.inbound) {
            onMessage(msgBytes.message())
        }
    }

    suspend fun processOutbound(conn: ClientConnection) {
        for (msg in outbound) {
            conn.outbound.send(msg.toByteArray())
        }
    }

    abstract suspend fun processAuth(conn: ClientConnection, challenge: Messages.AuthChallenge)
}

@ExperimentalTime
internal class AnonymousConnectionHandler constructor(client: Client, outboundBufferDepth: Int) :
    ConnectionHandler(client, outboundBufferDepth) {
    override suspend fun processAuth(conn: ClientConnection, challenge: Messages.AuthChallenge) {
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

@ExperimentalTime
internal class AuthenticatedConnectionHandler(
    client: Client,
    outboundBufferDepth: Int,
    inboundBufferDepth: Int
) : ConnectionHandler(client, outboundBufferDepth) {
    val inbound = Channel<InboundMessage>(inboundBufferDepth)

    override suspend fun processAuth(conn: ClientConnection, challenge: Messages.AuthChallenge) {
        val identityKeyPair = client.store.identityKeyPair
        val login = Messages.Login.newBuilder()
            .setNonce(challenge.nonce)
            .setAddress(
                Messages.Address.newBuilder()
                    .setIdentityKey(identityKeyPair.publicKey.bytes.byteString())
                    .setDeviceId(client.store.deviceId.bytes.byteString())
            ).build()
        val loginBytes = login.toByteArray()
        val signature = Curve.calculateSignature(identityKeyPair.privateKey, loginBytes)
        val authResponse = Messages.Message.newBuilder().setAuthResponse(
            Messages.AuthResponse.newBuilder()
                .setLogin(loginBytes.byteString())
                .setSignature(signature.byteString())
        ).build()
        conn.outbound.send(authResponse.toByteArray())
        val result = conn.inbound.receive().message()
        if (result.hasAck()) {
            // we're logged in!
        } else {
            // TODO: the below causes a crash. Would be good to handle that more cleanly
            throw Exception("Unable to log in: ${result.error.description}")
        }
    }

    override suspend fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.PREKEYSLOW -> {
                val newPreKeys = client.store.generatePreKeys(msg.preKeysLow.keysRequested)
                // TODO: actually send in the pre-keys

            }
            Messages.Message.PayloadCase.INBOUNDMESSAGE -> inbound.send(
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
@ExperimentalTime
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