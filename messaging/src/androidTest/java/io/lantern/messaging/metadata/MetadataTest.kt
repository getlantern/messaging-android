package io.lantern.messaging.metadata

import android.os.Build
import io.lantern.messaging.BaseTest
import io.lantern.messaging.Model
import java.lang.StringBuilder
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import org.junit.Test

class MetadataTest : BaseTest() {
    @Test
    fun testJpg() {
        val file = assetToFile("image.jpg")
        val md = Metadata.analyze(file)
        assertEquals("image/jpeg", md.mimeType)
        assertNotNull(md.thumbnail)
        assertTrue(md.thumbnail!!.size < file.length())
        assertEquals("image/webp", md.thumbnailMimeType)
    }

    @Test
    fun testHeic() {
        val file = assetToFile("image.heic")
        val md = Metadata.analyze(file)
        assertEquals("image/heic", md.mimeType)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            assertNotNull(md.thumbnail)
            assertTrue(md.thumbnail!!.size < file.length())
            assertEquals("image/webp", md.thumbnailMimeType)
        } else {
            assertNull(md.thumbnail)
        }
    }

    @Test
    fun testVideo() {
        doTests("video.mp4", "0")
    }

    private fun doTests(fileName: String, expectedRotation: String) {
        val file = assetToFile(fileName)
        val md = Metadata.analyze(file)
        assertEquals("video/mp4", md.mimeType)
        assertNotNull(md.thumbnail)
        assertTrue(md.thumbnail!!.size <= file.length())
        assertEquals("image/webp", md.thumbnailMimeType)
        // test additionalMetadata - should exist
        val additionalMetadata = md.additionalMetadata
        assertNotNull(additionalMetadata)
        // test rotation - should exist and be 0
        val rotation = additionalMetadata["rotation"]
        assertNotNull(rotation)
        assertEquals(expectedRotation, rotation)
    }

    @Test
    fun testUpsideDownVideo() {
        doTests("upside_down_test.mp4", "180")
    }

    @Test
    fun testAudio() {
        val file = assetToFile("clap.opus")
        val md = Metadata.analyze(file)
        assertNotNull(md)
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) {
            assertNull(md.thumbnail)
        } else {
            assertNotNull(md)
            assertEquals("audio/opus", md.mimeType)
            assertNotNull(md.thumbnail)
            assertTrue(md.thumbnail!!.size < file.length())
            assertEquals("application/x-lantern-waveform", md.thumbnailMimeType)

            // The audio file contains mostly silence and a single loud clap. Make sure that the
            // waveform reflects this by having a much higher peak than average value.
            val bars = Model.AudioWaveform.parseFrom(md.thumbnail).barsList
            val average = bars.average()
            val peak = bars.maxOrNull()!!
            assertEquals(255, peak)
            assertTrue(peak.toDouble() / average > 100)
            assertEquals("8.853", md.additionalMetadata?.get("duration"))

            printWaveform(bars)
        }
    }

    @Test
    fun testAudioLargeMp3() {
        val file = assetToFile("test.mp3")
        val md = Metadata.analyze(file)
        assertNotNull(md)
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) {
            assertNull(md.thumbnail)
        } else {
            assertNotNull(md)
            assertEquals("audio/mpeg", md.mimeType)
            assertNotNull(md.thumbnail)
            assertTrue(md.thumbnail!!.size < file.length())
            assertEquals("application/x-lantern-waveform", md.thumbnailMimeType)

            val expected = Model.AudioWaveform.parseFrom(md.thumbnail).barsList.joinToString()
            // calculate metadata again to make sure waveform generation is repeatable on the same
            // device
            val nextMd = Metadata.analyze(file)
            val bars = Model.AudioWaveform.parseFrom(nextMd.thumbnail).barsList
            val actual = bars.joinToString()
            assertEquals(expected, actual)

            printWaveform(bars)
        }
    }

    private fun printWaveform(bars: List<Int>) {
        // print out the waveform for visual inspection
        val builder = StringBuilder()
        for (i in 0..255) {
            val referenceLevel = 255 - i
            builder.append("$referenceLevel    ")
            bars.forEach {
                val level = it
                if (level >= referenceLevel) {
                    builder.append('A')
                } else {
                    builder.append(' ')
                }
            }
            builder.append('\n')
        }

        println("Waveform display")
        println(builder.toString())
    }
}
