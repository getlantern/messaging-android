package io.lantern.messaging.metadata

import android.os.Build
import io.lantern.messaging.BaseTest
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
        assertNull(md.mimeType)
        assertNull(md.thumbnail)
    }

    @Test
    fun testHeicWithDefaultMimeType() {
        val file = assetToFile("image.heic")
        val md = Metadata.analyze(file, "image/heic")
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
        val file = assetToFile("video.mp4")
        val md = Metadata.analyze(file)
        assertEquals("video/mp4", md.mimeType)
        assertNotNull(md.thumbnail)
        assertTrue(md.thumbnail!!.size <= file.length())
        assertEquals("image/webp", md.thumbnailMimeType)
    }
}
