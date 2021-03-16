package io.lantern.messaging.tassis

/**
 * Padding provides a scheme for padding messages prior to transmission, following the same
 * algorithm as used in the Signal application.
 */
object Padding {
    fun stripMessagePadding(messageWithPadding: ByteArray): ByteArray {
        var paddingStart = 0
        for (i in messageWithPadding.indices.reversed()) {
            if (messageWithPadding[i] == 0x80.toByte()) {
                paddingStart = i
                break
            } else if (messageWithPadding[i] != 0x00.toByte()) {
                logger.warn("Padding byte is malformed, returning unstripped padding.")
                return messageWithPadding
            }
        }
        val strippedMessage = ByteArray(paddingStart)
        System.arraycopy(messageWithPadding, 0, strippedMessage, 0, strippedMessage.size)
        return strippedMessage
    }

    fun padMessage(messageBody: ByteArray): ByteArray {
        // NOTE: This is dumb.  We have our own padding scheme, but so does the cipher.
        // The +1 -1 here is to make sure the Cipher has room to add one padding byte,
        // otherwise it'll add a full 16 extra bytes.
        val paddedMessage = ByteArray(getPaddedMessageLength(messageBody.size + 1) - 1)
        System.arraycopy(messageBody, 0, paddedMessage, 0, messageBody.size)
        paddedMessage[messageBody.size] = 0x80.toByte()
        return paddedMessage
    }

    private fun getPaddedMessageLength(messageLength: Int): Int {
        val messageLengthWithTerminator = messageLength + 1
        var messagePartCount = messageLengthWithTerminator / 160
        if (messageLengthWithTerminator % 160 != 0) {
            messagePartCount++
        }
        return messagePartCount * 160
    }
}