package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.db.DB
import io.lantern.messaging.store.MessagingProtocolStore
import org.junit.After
import org.junit.Before
import java.io.File
import java.util.*

abstract class BaseMessagingTest {
    protected var tempDir: File? = null

    protected val newDB: DB
        get() = DB.createOrOpen(
            InstrumentationRegistry.getInstrumentation().targetContext,
            File(tempDir, UUID.randomUUID().toString()).toString(),
            "password"
        )

    protected fun newStore(db: DB): MessagingProtocolStore =
        MessagingProtocolStore(db)

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