package io.lantern.messaging

import io.lantern.messaging.tassis.AuthenticatedClient
import io.lantern.messaging.tassis.AuthenticatedClientDelegate
import io.lantern.messaging.tassis.InboundMessage
import io.lantern.messaging.tassis.TransportFactory
import org.whispersystems.libsignal.ecc.Curve

internal class AuthenticatedClientWorker(
    transportFactory: TransportFactory,
    messaging: Messaging,
    private val roundTripTimeoutMillis: Long,
    redialBackoffMillis: Long,
    maxRedialDelayMillis: Long
) : ClientWorker<AuthenticatedClientDelegate, AuthenticatedClient>(
    transportFactory,
    messaging,
    "authenticated",
    redialBackoffMillis,
    maxRedialDelayMillis,
    autoConnect = true
), AuthenticatedClientDelegate {
    override fun buildClient(): AuthenticatedClient {
        return AuthenticatedClient(messaging.identityKeyPair.publicKey, messaging.deviceId, this, roundTripTimeoutMillis)
    }

    override fun signLogin(loginBytes: ByteArray): ByteArray {
        logger.debug("signing login")
        return Curve.calculateSignature(
            messaging.identityKeyPair.privateKey,
            loginBytes
        )
    }

    override fun onPreKeysLow(numPreKeysRequested: Int) {
        messaging.cryptoWorker.registerPreKeys(numPreKeysRequested)
    }

    override fun onInboundMessage(msg: InboundMessage) {
        messaging.cryptoWorker.decryptAndStore(msg)
    }
}