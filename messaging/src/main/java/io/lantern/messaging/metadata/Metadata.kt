package io.lantern.messaging.metadata

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Matrix
import android.media.ExifInterface
import android.media.MediaMetadataRetriever
import android.os.Build
import androidx.core.graphics.scale
import com.j256.simplemagic.ContentInfoUtil
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream

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
                mimeType?.startsWith("image") == true -> rotatedBitmap(file)
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
            return Metadata(mimeType, thumbnail, thumbnail.let { "image/webp" })
        }

        /**
         * This is needed to fix orientation of the thumbnails on some phones.
         * See https://github.com/google/cameraview/issues/22#issuecomment-363047917.
         */
        private fun rotatedBitmap(file: File): Bitmap? {
            val bmp = BitmapFactory.decodeFile(file.absolutePath)
            if (bmp == null || Build.VERSION.SDK_INT < Build.VERSION_CODES.N) {
                return bmp
            }
            val exif = ExifInterface(FileInputStream(file))
            val orientation: Int = exif.getAttributeInt(
                ExifInterface.TAG_ORIENTATION,
                ExifInterface.ORIENTATION_UNDEFINED
            )
            val matrix = Matrix()
            when (orientation) {
                ExifInterface.ORIENTATION_NORMAL -> return bmp
                ExifInterface.ORIENTATION_FLIP_HORIZONTAL -> matrix.setScale(-1.0f, 1.0f)
                ExifInterface.ORIENTATION_ROTATE_180 -> matrix.setRotate(180f)
                ExifInterface.ORIENTATION_FLIP_VERTICAL -> {
                    matrix.setRotate(180f)
                    matrix.postScale(-1f, 1f)
                }
                ExifInterface.ORIENTATION_TRANSPOSE -> {
                    matrix.setRotate(90f)
                    matrix.postScale(-1f, 1f)
                }
                ExifInterface.ORIENTATION_ROTATE_90 -> matrix.setRotate(90f)
                ExifInterface.ORIENTATION_TRANSVERSE -> {
                    matrix.setRotate(-90f)
                    matrix.postScale(-1f, 1f)
                }
                ExifInterface.ORIENTATION_ROTATE_270 -> matrix.setRotate(-90f)
            }
            val bmpRotated: Bitmap = Bitmap.createBitmap(
                bmp,
                0,
                0,
                bmp.getWidth(),
                bmp.getHeight(),
                matrix,
                true
            )
            bmp.recycle()
            return bmpRotated
        }

        private fun scaledThumbnail(
            bmp: Bitmap?,
            maxHeight: Int = 1000,
            maxWidth: Int = 1000
        ): ByteArray? {
            if (bmp == null) {
                return null
            }
            val targetRatio = maxHeight.toFloat() / maxWidth.toFloat()
            val actualRatio = bmp.height.toFloat() / bmp.width.toFloat()
            val scaled = if (bmp.height <= maxHeight && bmp.width <= maxWidth)
                bmp
            else if (actualRatio > targetRatio)
            // scale by height
                bmp.scale((maxHeight / actualRatio).toInt(), maxHeight)
            else
                bmp.scale(maxWidth, (maxWidth * actualRatio).toInt())
            val out = ByteArrayOutputStream()
            // We use WEBP for backward compatibility with older Android versions
            scaled.compress(Bitmap.CompressFormat.WEBP, 90, out)
            bmp.recycle()
            return out.toByteArray()
        }
    }
}
