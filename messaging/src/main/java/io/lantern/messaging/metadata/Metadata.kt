package io.lantern.messaging.metadata

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Matrix
import android.media.ExifInterface
import android.media.MediaCodec
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMetadataRetriever
import android.os.Build
import androidx.core.graphics.scale
import com.j256.simplemagic.ContentInfoUtil
import io.lantern.messaging.Model
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.math.RoundingMode
import java.util.concurrent.Executors
import kotlin.math.abs
import kotlin.math.floor
import mu.KotlinLogging

private val logger = KotlinLogging.logger {}

/**
 * Provides a facility for extracting content metadata while copying it
 */
class Metadata(
    val mimeType: String?,
    val thumbnail: ByteArray?,
    val thumbnailMimeType: String?,
    val additionalMetadata: Map<String, String>? = null
) {

    companion object {
        private const val BAR_COUNT = 1000
        private const val SAMPLES_PER_BAR = 4
        private const val MAX_QUANTIZED_VALUE = 255
        private val audioDecoderExecutor = Executors.newSingleThreadExecutor {
            Thread(it, "Metadata-audioDecoder")
        }

        private val util = ContentInfoUtil()

        /**
         * Obtains metadata by analyzing the given file.
         *
         * @param file the file to analyze
         * @param defaultMimeType the default mime type to use if one couldn't be detected
         */
        fun analyze(file: File, defaultMimeType: String? = null): Metadata {
            val mimeType = try {
                util.findMatch(file)?.mimeType
            } catch (t: Throwable) {
                null
            } ?: defaultMimeType

            try {
                return when (mimeType) {
                    "application/ogg" -> audioMetadata(file, mimeType)
                    "audio/ogg" -> audioMetadata(file, mimeType)
                    "audio/opus" -> audioMetadata(file, mimeType)
                    "audio/mp4" -> audioMetadata(file, mimeType)
                    "audio/m4a" -> audioMetadata(file, mimeType)
                    "audio/mkv" -> audioMetadata(file, mimeType)
                    "audio/mp3" -> audioMetadata(file, mimeType)
                    "audio/flac" -> audioMetadata(file, mimeType)
                    "audio/mpeg" -> audioMetadata(file, mimeType)
                    else -> visualMetadata(file, mimeType)
                }
            } catch (t: Throwable) {
                logger.error("couldn't extract thumbnail: ${t.message}")
                return Metadata(mimeType, null, null)
            }
        }

        private fun visualMetadata(file: File, mimeType: String?): Metadata {
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
            return Bitmap.createBitmap(
                bmp,
                0,
                0,
                bmp.width,
                bmp.height,
                matrix,
                true
            )
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
            return out.toByteArray()
        }

        private fun audioMetadata(file: File, defaultMimeType: String): Metadata {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) {
                // waveform generation not supported on older Android versions, return null
                return Metadata(defaultMimeType, null, null)
            }

            // The below code is based on AudioWaveForm.java from Signal-Android.
            // It generates Audio WaveForms consisting of BAR_COUNT amplitude bars with each bar
            // encoded as a byte in a ByteArray.
            val extractor = MediaExtractor()
            extractor.setDataSource(file.absolutePath)
            if (extractor.trackCount == 0) {
                throw IOException("No audio track")
            }
            val wave = LongArray(BAR_COUNT)
            val waveSamples = IntArray(BAR_COUNT)
            val format = extractor.getTrackFormat(0)
            if (!format.containsKey(MediaFormat.KEY_DURATION)) {
                throw IOException("Unknown duration")
            }
            val totalDurationUs = format.getLong(MediaFormat.KEY_DURATION)
            val mimeType = format.getString(MediaFormat.KEY_MIME)
            if (!mimeType!!.startsWith("audio/")) {
                throw IOException("Mime not audio")
            }
            val codec = MediaCodec.createDecoderByType(mimeType)
            if (totalDurationUs == 0L) {
                throw IOException("Zero duration")
            }
            codec.configure(format, null, null, 0)
            codec.start()
            extractor.selectTrack(0)
            val kTimeOutUs: Long = 5000 // 5000 microseconds (5 milliseconds)

            audioDecoderExecutor.submit {
                var sawInputEOS = false
                while (!sawInputEOS) {
                    val inputBufIndex = codec.dequeueInputBuffer(kTimeOutUs)
                    if (inputBufIndex >= 0) {
                        val dstBuf = codec.getInputBuffer(inputBufIndex)!!
                        var sampleSize = extractor.readSampleData(dstBuf, 0)
                        var presentationTimeUs: Long = 0
                        if (sampleSize < 0) {
                            sawInputEOS = true
                            sampleSize = 0
                        } else {
                            presentationTimeUs = extractor.sampleTime
                        }
                        if (!sawInputEOS) {
                            val barSampleIndex =
                                (
                                    SAMPLES_PER_BAR * (BAR_COUNT * extractor.sampleTime) /
                                        totalDurationUs
                                    ).toInt()
                            sawInputEOS = !extractor.advance()
                            var nextBarSampleIndex =
                                (
                                    SAMPLES_PER_BAR * (BAR_COUNT * extractor.sampleTime) /
                                        totalDurationUs
                                    ).toInt()
                            while (!sawInputEOS && nextBarSampleIndex == barSampleIndex) {
                                sawInputEOS = !extractor.advance()
                                if (!sawInputEOS) {
                                    nextBarSampleIndex =
                                        (
                                            SAMPLES_PER_BAR * (BAR_COUNT * extractor.sampleTime) /
                                                totalDurationUs
                                            ).toInt()
                                }
                            }
                        }

                        codec.queueInputBuffer(
                            inputBufIndex,
                            0,
                            sampleSize,
                            presentationTimeUs,
                            if (sawInputEOS) MediaCodec.BUFFER_FLAG_END_OF_STREAM else 0
                        )
                    }
                }
            }

            val info = MediaCodec.BufferInfo()
            var sawOutputEOS = false
            while (!sawOutputEOS) {
                var outputBufferIndex = 0
                while (!sawOutputEOS && outputBufferIndex >= -1) {
                    outputBufferIndex = codec.dequeueOutputBuffer(info, kTimeOutUs)
                    if (outputBufferIndex >= 0) {
                        val buf = codec.getOutputBuffer(outputBufferIndex)!!
                        val barIndex =
                            (BAR_COUNT * info.presentationTimeUs / totalDurationUs).toInt()
                        var total: Long = 0
                        var i = 0
                        while (i < info.size) {
                            val aShort = buf.getShort(i)
                            total += abs(aShort.toInt()).toLong()
                            i += 2 * 4
                        }
                        if (barIndex >= 0 && barIndex < BAR_COUNT) {
                            wave[barIndex] += total
                            waveSamples[barIndex] += info.size / 2
                        }
                        if (info.flags and MediaCodec.BUFFER_FLAG_END_OF_STREAM != 0) {
                            sawOutputEOS = true
                        }
                        codec.releaseOutputBuffer(outputBufferIndex, false)
                    } else if (outputBufferIndex == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                        logger.debug("output format has changed to " + codec.outputFormat)
                    }
                }
            }

            codec.stop()
            codec.release()
            extractor.release()
            val floats = FloatArray(BAR_COUNT)
            val ints = IntArray(BAR_COUNT)
            var max = 0f
            for (i in 0 until BAR_COUNT) {
                if (waveSamples[i] == 0) continue
                floats[i] = wave[i] / waveSamples[i].toFloat()
                if (floats[i] > max) {
                    max = floats[i]
                }
            }
            for (i in 0 until BAR_COUNT) {
                val normalized = floats[i] / max
                ints[i] = (floor(MAX_QUANTIZED_VALUE * normalized.toDouble())).toInt()
            }

            // convert duration from microseconds to seconds
            val durationSeconds = (totalDurationUs.toDouble() / 1000000.0)
                .toBigDecimal()
                .setScale(3, RoundingMode.HALF_EVEN)
            return Metadata(
                mimeType,
                Model.AudioWaveform.newBuilder().addAllBars(ints.toList()).build().toByteArray(),
                "application/x-lantern-waveform",
                mapOf("duration" to durationSeconds.toString())
            )
        }
    }
}
