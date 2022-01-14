package io.lantern.messaging

import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.HashMap
import org.whispersystems.libsignal.ecc.ECPublicKey

/**
 * Provides a human-friendly encoding that looks like a phone number but isn't usually a dialable
 * phone number, because it doesn't start with 0 or 1 (as is required in most countries). This
 * encoding treats a byte array as a big-endian number. The first (most significant) 2 bits of data
 * are encoded using a modified base4 encoding (digits 2, 3, 4, 6 instead of the standard 0-4) and the
 * remaining data is encoded in base9 (omitting digit 5) and left-padded with '0's to meet the desired length.
 *
 * This encoding permits the inclusion of arbitrary '5's anywhere in the encoded string, which it simply ignores. This
 * can be used to visually differentiate the beginning of two otherwise very similar numbers, for example, given:
 *
 * 2222222222222222222222222222222222222222222222222222222222222222222222222222222
 * 2222222222222222222222222222222222222222222222222222222222222222222222222222223
 *
 * We can change the 2nd number to the following equivalent value
 *
 * 522222222222252222222222222222222222222222222222222222222222222222222222222222223
 */
object ChatNumberEncoding {
    private val base4Table: MutableMap<Byte, Char> = HashMap()
    private val base4TableReverse: MutableMap<Char, Int> = HashMap()

    init {
        addBase4Mapping(0, '2')
        addBase4Mapping(1, '3')
        addBase4Mapping(2, '4')
        addBase4Mapping(3, '6')
    }

    private fun addBase4Mapping(b: Byte, c: Char) {
        base4Table[b] = c
        base4TableReverse[c] = byteToUnsigned(b)
    }

    /**
     * Encodes the given bytes using ChatNumber encoding. The resulting string will be of target
     * length using '0's after the first digit in order to pad up to the targetLength.
     *
     * @param b
     * @param targetLength
     * @return
     */
    fun encodeToString(b: ByteArray, targetLength: Int): String {
        val cb = b.copyOf()
        val head = (byteToUnsigned(cb[0]) ushr 6).toByte()
        cb[0] = (byteToUnsigned(cb[0]) shl 2).toByte()
        val tail = shiftBase9(BigInteger(1, cb).toString(9))
        val result = StringBuilder(targetLength)
        result.append(base4Table[head])
        var padding = targetLength - 1 - tail.length
        if (padding < 0) {
            padding = 0
        }
        for (i in 0 until padding) {
            result.append('0')
        }
        result.append(tail)
        return result.toString()
    }

    /**
     * Decodes the given ChatNumber string into a byte[] of targetSize. If the string doesn't
     * contain enough data to fill targetSize, the byte[] will contain leading zeros.
     *
     * This function ignores any leading characters other than 2, 3, 4 or 6, and any subsequent 5s.
     */
    fun decodeFromString(string: String, targetSize: Int): ByteArray {
        val s = string.replace("^[015789]+".toRegex(), "")
        val head = base4TableReverse[s[0]]!!
        var itail = BigInteger(unshiftBase9(s.substring(1)), 9)
        val buf = ByteBuffer.allocate(targetSize)
        for (i in targetSize / 4 - 1 downTo 0) {
            buf.putInt(i * 4, itail.toInt())
            itail = itail.shiftRight(32)
        }
        val tail = buf.array()
        tail[0] = (head shl 6 or (byteToUnsigned(tail[0]) ushr 2)).toByte()
        return tail
    }

    /**
     * Builds an ECPublicKey from the given chatNumber.
     */
    fun identityKey(chatNumber: String): ECPublicKey =
        ECPublicKey(decodeFromString(chatNumber, 32))

    private fun shiftBase9(s: String): String {
        val result = StringBuilder(s.length)
        for (c in s) {
            if (c < '5') {
                result.append(c)
            } else {
                result.append((c.code + 1).toChar())
            }
        }
        return result.toString()
    }

    private fun unshiftBase9(s: String): String {
        val result = StringBuilder(s.length)
        for (c in s) {
            when {
                c < '5' -> {
                    result.append(c)
                }
                c == '5' -> {
                    // ignore 5s
                }
                else -> {
                    result.append((c.code - 1).toChar())
                }
            }
        }
        return result.toString()
    }

    private fun byteToUnsigned(b: Byte): Int {
        return b.toInt() and 0xFF
    }
}
