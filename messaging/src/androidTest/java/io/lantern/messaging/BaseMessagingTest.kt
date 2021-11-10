package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.db.DB
import java.io.File
import java.util.UUID

abstract class BaseMessagingTest : BaseTest() {
    protected val newDB: DB
        get() = DB.createOrOpen(
            InstrumentationRegistry.getInstrumentation().targetContext,
            File(tempDir, UUID.randomUUID().toString()).toString(),
            arrayOf(1.toByte(), 2.toByte(), 3.toByte()).toByteArray()
        )
}
