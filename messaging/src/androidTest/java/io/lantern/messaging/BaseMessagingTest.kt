package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.messaging.store.MessagingStore
import org.junit.After
import org.junit.Before
import java.io.File
import java.util.*

abstract class BaseMessagingTest {
    private var tempDir: File? = null

    protected fun newStore(name: String? = null): MessagingStore = MessagingStore(
        InstrumentationRegistry.getInstrumentation().targetContext,
        dbPath = File(tempDir, name ?: UUID.randomUUID().toString()).toString()
    )

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
}