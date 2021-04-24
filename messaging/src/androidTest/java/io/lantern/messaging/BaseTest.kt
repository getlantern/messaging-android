package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import org.junit.After
import org.junit.Before
import java.io.File
import java.io.FileOutputStream
import java.util.Random

abstract class BaseTest {
    protected var tempDir: File? = null

    @Before
    fun setupTempDir() {
        tempDir = File(
            InstrumentationRegistry.getInstrumentation().targetContext.cacheDir,
            Random().nextLong().toString()
        )
        tempDir!!.mkdirs()
    }

    @After
    fun deleteTempDir() {
        tempDir?.deleteRecursively()
    }

    protected fun assetToFile(name: String): File {
        val file = File(tempDir, name)
        FileOutputStream(file).use { out ->
            InstrumentationRegistry.getInstrumentation().context.assets
                .open(name).copyTo(out, 1024)
        }
        return file
    }
}
