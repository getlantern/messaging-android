package io.lantern.messaging

/**
 * A WebRTC signal from another user
 */
class WebRTCSignal(val senderId: String, val deviceId: String, val content: ByteArray)
