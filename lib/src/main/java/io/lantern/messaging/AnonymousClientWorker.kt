package io.lantern.messaging

import io.lantern.messaging.tassis.AnonymousClient
import io.lantern.messaging.tassis.AnonymousClientDelegate
import io.lantern.messaging.tassis.TransportFactory

internal class AnonymousClientWorker(
    transportFactory: TransportFactory,
    messaging: Messaging,
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
        return AnonymousClient(this)
    }
}