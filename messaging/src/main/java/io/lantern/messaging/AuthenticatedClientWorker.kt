package io.lantern.messaging

import io.lantern.messaging.tassis.AuthenticatedClient
import io.lantern.messaging.tassis.AuthenticatedClientDelegate
import io.lantern.messaging.tassis.InboundMessage
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.TransportFactory
import org.whispersystems.libsignal.ecc.Curve

internal class AuthenticatedClientWorker(
    transportFactory: TransportFactory,
    messaging: Messaging,
    private val roundTripTimeoutMillis: Long,
    redialBackoffMillis: Long,
    maxRedialDelayMillis: Long,
    retryDelayMillis: Long
) : ClientWorker<AuthenticatedClientDelegate, AuthenticatedClient>(
    transportFactory,
    messaging,
    "authenticated",
    roundTripTimeoutMillis,
    redialBackoffMillis,
    maxRedialDelayMillis,
    retryDelayMillis,
    autoConnect = true
),
    AuthenticatedClientDelegate {
    override fun buildClient(): AuthenticatedClient {
        return AuthenticatedClient(
            messaging.identityKeyPair.publicKey,
            messaging.deviceId, this,
            roundTripTimeoutMillis
        )
    }

    override fun signLogin(loginBytes: ByteArray): ByteArray {
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

    override fun onConfigUpdate(cfg: Messages.Configuration) {
        messaging.updateConfig(cfg)
    }

    override fun onConnected(client: AuthenticatedClient, number: Messages.ChatNumber) {
        super.onConnected(client)
        messaging.db.mutate { tx ->
            tx.get<Model.Contact>(Schema.PATH_ME)?.let { me ->
                // only set number once
                if (!me.hasChatNumber()) {
                    tx.put(
                        Schema.PATH_ME,
                        me.toBuilder().setChatNumber(number.pbuf).build()
                    )
                }
            }
        }
    }
}
