package io.lantern.messaging.metadata

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.media.MediaMetadataRetriever
import androidx.core.graphics.scale
import com.j256.simplemagic.ContentInfoUtil
import java.io.ByteArrayOutputStream
import java.io.File

/**
 * Provides a facility for extracting content metadata while copying it
 */
class Metadata(val mimeType: String?, val thumbnail: ByteArray?, val thumbnailMimeType: String?) {

    companion object {
        private val util = ContentInfoUtil()

        /**
         * Obtains metadata by analyzing the given file.
         *
         * @param file the file to analyze
         * @param defaultMimeType the default mime type to use if one couldn't be detected
         */
        fun analyze(file: File, defaultMimeType: String? = null): Metadata {
            val mimeType = util.findMatch(file)?.mimeType ?: defaultMimeType
            val bmp = when {
                mimeType?.startsWith("image") == true -> BitmapFactory.decodeFile(file.absolutePath)
                mimeType?.startsWith("video") == true -> {
                    val retriever = MediaMetadataRetriever()
                    try {
                        retriever.setDataSource(file.absolutePath)
                        retriever.getFrameAtTime(0)
                    } finally {
                        retriever.release()
                    }
                }
                else -> null
            }
            val thumbnail = scaledThumbnail(bmp)
            return Metadata(mimeType, thumbnail, thumbnail.let { "image/jpeg" })
        }

        private fun scaledThumbnail(
            bmp: Bitmap?,
            maxHeight: Int = 200,
            maxWidth: Int = 200
        ): ByteArray? {
            if (bmp == null) {
                return null
            }
            val targetRatio = maxHeight.toFloat() / maxWidth.toFloat()
            val actualRatio = bmp.height.toFloat() / bmp.width.toFloat()
            val scaled = if (actualRatio > targetRatio)
            // scale by height
                bmp.scale((maxHeight / actualRatio).toInt(), maxHeight)
            else
                bmp.scale(maxWidth, (maxWidth * actualRatio).toInt())
            val out = ByteArrayOutputStream()
            // We use WEBP for backward compatibility with older Android versions
            scaled.compress(Bitmap.CompressFormat.WEBP, 90, out)
            return out.toByteArray()
        }
    }
}
