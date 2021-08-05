package io.lantern.messaging.metadata

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Matrix
import android.media.MediaCodec
import android.media.MediaExtractor
import android.media.MediaFormat
import android.media.MediaMetadataRetriever
import android.os.Build
import androidx.core.graphics.scale
import androidx.exifinterface.media.ExifInterface
import com.j256.simplemagic.ContentInfoUtil
import io.lantern.messaging.Model
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.math.RoundingMode
import java.nio.ByteBuffer
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
        private const val numberOfBars = 1000 // resolution of waveform in bars
        private const val maxQuantizedValue = 255 // the maximum value of a bar
        private const val bytesPerSample = 2 // decoded audio is PCM16, 2 bytes per sample
        private const val microsecondsPerSecond = 1000000.0
        private const val dequeueTimeoutUs = 5000L // 5000 microseconds (5 milliseconds)
        private const val maxBytesToProcess = 20000L // don't process more than this many bytes

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
            val format = extractor.getTrackFormat(0)
            if (!format.containsKey(MediaFormat.KEY_DURATION)) {
                throw IOException("Unknown duration")
            }
            val totalDurationUs = format.getLong(MediaFormat.KEY_DURATION)
            val mimeType = format.getString(MediaFormat.KEY_MIME)
            if (mimeType!! != "application/ogg" && !mimeType!!.startsWith("audio/")) {
                throw IOException("Mime not audio")
            }
            val fileSize = file.length()
            val bytesPerBar = fileSize / numberOfBars
            val targetBytesPerBar = Math.min(fileSize, maxBytesToProcess) / numberOfBars
            val codec = MediaCodec.createDecoderByType(mimeType)
            if (totalDurationUs == 0L) {
                throw IOException("Zero duration")
            }
            codec.configure(format, null, null, 0)
            codec.start()
            extractor.selectTrack(0)

            audioDecoderExecutor.submit {
                // read all encoded samples (mp3, opus, whatever) and submit them to the MediaCodec
                // for decoding
                var sawInputEOS = false
                var scratchBuffer = ByteBuffer.allocate(1024768)
                var bytesProcessedForCurrentBar = 0
                while (!sawInputEOS) {
                    val inputBufIndex = codec.dequeueInputBuffer(dequeueTimeoutUs)
                    if (inputBufIndex >= 0) {
                        val dstBuf = codec.getInputBuffer(inputBufIndex)!!
                        var sampleSize = extractor.readSampleData(dstBuf, 0)
                        bytesProcessedForCurrentBar += sampleSize
                        var presentationTimeUs = 0L
                        if (sampleSize < 0) {
                            sawInputEOS = true
                            sampleSize = 0
                        } else {
                            presentationTimeUs = extractor.sampleTime
                        }

                        if (!sawInputEOS) {
                            sawInputEOS = !extractor.advance()

                            if (bytesProcessedForCurrentBar >= targetBytesPerBar) {
                                // on large files, this will skip some data in order to save CPU and
                                // speed up waveform generation
                                while (!sawInputEOS && bytesProcessedForCurrentBar < bytesPerBar) {
                                    println("skipping")
                                    bytesProcessedForCurrentBar += extractor.readSampleData(
                                        scratchBuffer,
                                        0
                                    )
                                    sawInputEOS = !extractor.advance()
                                }
                                bytesProcessedForCurrentBar = 0
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

            // read all decoded samples (assumed to be in 16 bit PCM format, possibly multiple
            // channels
            var decodedSamples = ArrayList<Long>()
            var decodedSampleTimesUs = ArrayList<Long>()
            var totalDecodedDurationUs = 0L

            var sawOutputEOS = false
            while (!sawOutputEOS) {
                var outputBufferIndex = 0
                while (!sawOutputEOS && outputBufferIndex >= -1) {
                    val info = MediaCodec.BufferInfo()
                    outputBufferIndex = codec.dequeueOutputBuffer(info, dequeueTimeoutUs)
                    if (outputBufferIndex >= 0) {
                        if (info.size > 0) {
                            val buf = codec.getOutputBuffer(outputBufferIndex)!!
                            var i = 0
                            var total = 0L
                            var numSamples = 0L
                            while (i < info.size) {
                                totalDecodedDurationUs = info.presentationTimeUs
                                total += abs(buf.getShort(i).toInt()).toLong()
                                numSamples += 1
                                i += bytesPerSample
                            }
                            decodedSamples.add(total / numSamples)
                            decodedSampleTimesUs.add(totalDecodedDurationUs)
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

            // When we skip data on the read end, the presentationTimeUs on decoded end is sometimes
            // incorrect. It's always correctly sequenced, but the timeline may appear much shorter.
            // So above, we just collected the individual samples and corresponding timestamps, and
            // here we scale them back to the actual duration of the file.
            val wave = LongArray(numberOfBars)
            val waveSamples = IntArray(numberOfBars)
            for (i in 0 until decodedSamples.size) {
                var ts = decodedSampleTimesUs.get(i)
                var sample = decodedSamples.get(i)
                val barIndex = (numberOfBars * ts / totalDecodedDurationUs).toInt()
                if (barIndex in 0 until numberOfBars) {
                    wave[barIndex] += sample
                    waveSamples[barIndex] += 1
                }
            }

            val floats = FloatArray(numberOfBars)
            val ints = IntArray(numberOfBars)
            var max = 0f

            // calculate float values by averaging all samples in each bar
            for (i in 0 until numberOfBars) {
                if (waveSamples[i] == 0) {
                    // We didn't get any data for this bar, skip it for now
                    continue
                }
                floats[i] = wave[i] / waveSamples[i].toFloat()
                if (floats[i] > max) {
                    max = floats[i]
                }
            }

            // normalize float values o na scale of 0 - 255
            for (i in 0 until numberOfBars) {
                val normalized = floats[i] / max
                ints[i] = (floor(maxQuantizedValue * normalized.toDouble())).toInt()
            }

            // convert duration from microseconds to seconds
            val durationSeconds = (totalDurationUs.toDouble() / microsecondsPerSecond)
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
