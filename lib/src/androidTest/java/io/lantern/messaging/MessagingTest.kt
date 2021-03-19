package io.lantern.messaging

import io.lantern.messaging.store.MessagingStore
import io.lantern.messaging.tassis.websocket.WebSocketTransportFactory
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.milliseconds
import kotlin.time.seconds

private val logger = KotlinLogging.logger {}

@ExperimentalTime
class MessagingTest : BaseMessagingTest() {

    @Test
    fun testCompleteFlow() {
        val catStore = newStore
        val dog = newMessaging("dog")

        runBlocking {
            dog.use { dog ->
                val catIdentity = catStore.identityKeyPair.publicKey
                val text = "hello cat"
                val msg = dog.send(text, null, catIdentity)
                var storedMsg = dog.waitFor<Model.ShortMessageRecord>(msg.dbPath) {
                    it?.status == Model.ShortMessageRecord.DeliveryStatus.FAILING
                }
                assertEquals(
                    Model.ShortMessageRecord.DeliveryStatus.FAILING,
                    storedMsg?.status,
                    "attempt to send to cat before cat has started registering preKeys should have resulted in a UserMessage with failing status"
                )

                val cat = newMessaging("cat", store = catStore)
                storedMsg =
                    dog.waitFor(msg.dbPath) { it?.status == Model.ShortMessageRecord.DeliveryStatus.SENT }
                assertEquals(
                    Model.ShortMessageRecord.DeliveryStatus.SENT,
                    storedMsg?.status,
                    "once cat has started registering preKeys, pending UserMessage should successfully send"
                )
            }
            logger.debug("test finished")

        }
        logger.debug("finished runBlocking")
    }

    private fun newMessaging(
        name: String,
        failedSendRetryDelay: Duration = 100.milliseconds,
        store: MessagingStore? = null
    ): Messaging {
        return Messaging(
            store ?: newStore,
            WebSocketTransportFactory("wss://tassis.lantern.io/api"),
            failedSendRetryDelay = failedSendRetryDelay,
            name = name
        )
    }
}

@ExperimentalTime
internal suspend fun <T : Any> Messaging.waitFor(
    path: String,
    duration: Duration = 5.seconds,
    check: (T?) -> Boolean
): T? {
    return waitFor(duration.toLongMilliseconds()) {
        val result: T? = this.store.db.get(path)
        if (check(result)) result else null
    }
}

private suspend fun <T> waitFor(maxWait: Long, get: suspend () -> T?): T? {
    var elapsed = 0
    while (elapsed < maxWait) {
        val result = get()
        if (result != null) {
            return result
        }
        delay(250)
        elapsed += 250
    }
    return null
}