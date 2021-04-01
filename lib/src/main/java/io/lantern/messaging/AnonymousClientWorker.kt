package io.lantern.messaging

import io.lantern.messaging.tassis.AnonymousClient
import io.lantern.messaging.tassis.AnonymousClientDelegate
import io.lantern.messaging.tassis.Messages
import io.lantern.messaging.tassis.TransportFactory

internal class AnonymousClientWorker(
    transportFactory: TransportFactory,
    messaging: Messaging,
    private val roundTripTimeoutMillis: Long,
    redialBackoffMillis: Long,
    maxRedialDelayMillis: Long
) : ClientWorker<AnonymousClientDelegate, AnonymousClient>(
    transportFactory,
    messaging,
    "anonymous",
    redialBackoffMillis,
    maxRedialDelayMillis
), AnonymousClientDelegate {
    override fun buildClient(): AnonymousClient {
        return AnonymousClient(this, roundTripTimeoutMillis)
    }

    override fun onConfigUpdate(cfg: Messages.Configuration) {
        messaging.updateConfig(cfg)
    }
}