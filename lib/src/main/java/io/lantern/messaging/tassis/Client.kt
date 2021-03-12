package io.lantern.messaging.tassis

import com.google.protobuf.ByteString
import com.google.protobuf.GeneratedMessageLite
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import mu.KotlinLogging
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.state.SignalProtocolStore
import java.nio.charset.Charset
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

private val logger = KotlinLogging.logger {}

interface Dialer {
    fun dial(): ClientConnection
}

interface ClientConnection {
    val outbound: SendChannel<ByteArray>
    val inbound: ReceiveChannel<ByteArray>
    val errors: ReceiveChannel<Throwable>

    fun close()
}

@ExperimentalTime
class Client(
    internal val protocolStore: SignalProtocolStore,
    private val dialer: Dialer,
    internal val scope: CoroutineScope,
    private val redialBackoff: Duration,
    private val maxRedialDelay: Duration,
    anonymousOutboundBufferDepth: Int = 100,
    authenticatedOutboundBufferDepth: Int = 10,
) {
    private val anonymousHandler = AnonymousConnectionHandler(this, anonymousOutboundBufferDepth)
    private val authenticatedHandler =
        AuthenticatedConnectionHandler(this, authenticatedOutboundBufferDepth)

    init {
        scope.launch {
            anonymousHandler.processInbound()
            authenticatedHandler.processInbound()
        }
    }

    internal suspend fun dial(): ClientConnection {
        var failures = 0
        while (true) {
            try {
                return dialer.dial()
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
    private val msgSequence = AtomicInteger()
    protected val pending = ConcurrentHashMap<Int, Callback<Any?>>()
    internal val outbound = Channel<Messages.Message>(outboundBufferDepth)

    protected fun send(detail: GeneratedMessageLite<*, *>, callback: Callback<*>? = null) {
        val msg = buildMessage(detail)
        if (callback != null) {
            pending[msg.sequence] = callback as Callback<Any?>
        }
        send(msg)
    }

    internal suspend fun send(conn: ClientConnection, msg: Messages.Message) {
        conn.outbound.send(msg.toByteArray())
    }

    private fun buildMessage(detail: GeneratedMessageLite<*, *>): Messages.Message {
        val msg = Messages.Message.newBuilder()
        when (detail) {
            is Messages.AuthResponse -> msg.authResponse = detail
            is Messages.Register -> msg.register = detail
            is Messages.Unregister -> msg.unregister = detail
            is Messages.RequestPreKeys -> msg.requestPreKeys = detail
            is Messages.OutboundMessage -> msg.outboundMessage = detail
            else -> throw Exception("attempted to send unknown message type ${msg.javaClass.name}")
        }
        return msg.build()
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
        val msg = conn.inbound.receive().toMessage()
        processAuth(conn, msg.authChallenge)
        client.scope.launch { processOutbound(conn) }
        for (msgBytes in conn.inbound) {
            onMessage(msgBytes.toMessage())
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
        // ignore
    }

    override suspend fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.PREKEYS -> pending.remove(msg.sequence)?.onSuccess(msg.preKeys)
            else -> return super.onMessage(msg)
        }
    }
}

@ExperimentalTime
internal class AuthenticatedConnectionHandler(
    client: Client,
    outboundBufferDepth: Int
) : ConnectionHandler(client, outboundBufferDepth) {
    override suspend fun processAuth(conn: ClientConnection, challenge: Messages.AuthChallenge) {
        val identityKeyPair = client.protocolStore.identityKeyPair
        val login = Messages.Login.newBuilder()
            .setNonce(challenge.nonce)
            .setAddress(
                Messages.Address.newBuilder()
                    .setIdentityKey(ByteString.copyFrom(identityKeyPair.publicKey.bytes))
                    .setDeviceId(ByteString.copyFrom("blah".toByteArray(Charset.defaultCharset())))
            ).build()
        val loginBytes = login.toByteArray()
        val signature = Curve.calculateSignature(identityKeyPair.privateKey, loginBytes)
        val authResponse = Messages.AuthResponse.newBuilder()
            .setLogin(ByteString.copyFrom(loginBytes))
            .setSignature(ByteString.copyFrom(signature)).build()
        conn.outbound.send(authResponse.toByteArray())
        val result = conn.inbound.receive().toMessage()
        if (result.hasAck()) {
            // we're logged in!
        } else {
            throw Exception("Unable to log in: ${result.error.description}")
        }
    }

    override suspend fun onMessage(msg: Messages.Message) {
        when (msg.payloadCase) {
            Messages.Message.PayloadCase.PREKEYSLOW -> {

            }
            // TODO: need to handle prekeys low
            Messages.Message.PayloadCase.INBOUNDMESSAGE -> ignore()
            else -> super.onMessage(msg)
        }
    }
}

fun ignore() {}

fun ByteArray.toMessage(): Messages.Message {
    return Messages.Message.parseFrom(this)
}