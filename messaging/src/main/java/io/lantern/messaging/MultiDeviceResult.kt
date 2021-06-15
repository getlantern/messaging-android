package io.lantern.messaging

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet
import java9.util.concurrent.CompletableFuture

/**
 * The result of sending something to multiple devices.
 */
class MultiDeviceResult(
    /** the deviceIDs to which the signal was successfully sent */
    val successfulDeviceIds: Set<String>,
    /** map of deviceID to Throwable for all device IDs to which the signal could not be sent */
    val errors: Map<String, Throwable>
) {
    /** Indicates if the signal was successfully sent to all known devices */
    val allSucceeded: Boolean get() = errors.isEmpty()

    internal class Builder(
        private val numberOfExpectedDevices: Int,
        private val future: CompletableFuture<MultiDeviceResult>
    ) {
        val successfulDeviceIDs = ConcurrentSkipListSet<String>()
        val errors = ConcurrentHashMap<String, Throwable>()

        internal fun deviceSucceded(deviceId: String) {
            successfulDeviceIDs.add(deviceId)
            attemptToComplete()
        }

        internal fun deviceFailed(deviceId: String, err: Throwable) {
            errors.put(deviceId, err)
            attemptToComplete()
        }

        private fun attemptToComplete() {
            if (successfulDeviceIDs.size + errors.size == numberOfExpectedDevices) {
                future.complete(MultiDeviceResult(successfulDeviceIDs, errors))
            }
        }
    }
}
