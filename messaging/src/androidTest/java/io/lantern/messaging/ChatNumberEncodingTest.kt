package io.lantern.messaging

import java.util.Random
import kotlin.test.assertEquals
import org.junit.Test
import org.whispersystems.libsignal.util.HFBase32

class ChatNumberEncodingTest {
    @Test
    fun testEncodeToString() {
        val b = HFBase32.decode(
            "rfu2495fqazzpq1e3xkj1skmr9785hwbxggpr17ut1htj4h9nhyy"
        )
        assertEquals(
            "2277029271600308397119018701998194490680040839333862997699030902896411310611021743",
            ChatNumberEncoding.encodeToString(b, 82)
        )
    }

    @Test
    fun testRoundTrip() {
        val random = Random()
        for (i in 0..9999) {
            val b = ByteArray(32)
            random.nextBytes(b)
            val expected = ChatNumberEncoding.encodeToString(b, 82)
            val actual = ChatNumberEncoding.encodeToString(
                ChatNumberEncoding.decodeFromString(
                    // insert spurious 5's to make sure they're ignored
                    "55${expected.substring(0, 12)}55${expected.substring(12)}",
                    32
                ),
                82
            )
            assertEquals(expected, actual)
        }
    }
}
