package io.lantern.messaging

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet

/**
 * The result of sending something to multiple devices.
 */
class MultiDeviceResult(
    /** overall error (not device specific) **/
    val error: Throwable? = null,
    /** deviceIDs to which the message was successfully sent */
    val successfulDeviceIds: Set<String>? = null,
    /** map of deviceID to Throwable for all device IDs to which the message could not be sent */
    val deviceErrors: Map<String, Throwable>? = null,
) {
    /** Indicates if the message was successfully sent to all known devices */
    val succeeded: Boolean get() = error == null && deviceErrors?.isEmpty() ?: true

    internal class Builder(
        private val numberOfExpectedDevices: Int,
        private val onComplete: (MultiDeviceResult) -> Unit
    ) {
        val successfulDeviceIds = ConcurrentSkipListSet<String>()
        val deviceErrors = ConcurrentHashMap<String, Throwable>()

        internal fun fail(err: Throwable) {
            onComplete(MultiDeviceResult(error = err))
        }

        internal fun deviceSucceded(deviceId: String) {
            successfulDeviceIds.add(deviceId)
            attemptToComplete()
        }

        internal fun deviceFailed(deviceId: String, err: Throwable) {
            deviceErrors.put(deviceId, err)
            attemptToComplete()
        }

        private fun attemptToComplete() {
            if (successfulDeviceIds.size + deviceErrors.size == numberOfExpectedDevices) {
                onComplete(
                    MultiDeviceResult(
                        successfulDeviceIds = successfulDeviceIds,
                        deviceErrors = deviceErrors
                    )
                )
            }
        }
    }

    companion object {
        internal fun fail(
            onComplete: (MultiDeviceResult) -> Unit,
            err: Throwable,
            deviceId: String? = null
        ) {
            val builder = Builder(
                if (deviceId != null) 1 else 0,
                onComplete
            )
            deviceId?.let {
                builder.deviceFailed(it, err)
            } ?: run { builder.fail(err) }
        }
    }
}
