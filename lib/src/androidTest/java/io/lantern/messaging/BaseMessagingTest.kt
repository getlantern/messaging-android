package io.lantern.messaging

import androidx.test.platform.app.InstrumentationRegistry
import io.lantern.messaging.store.MessagingStore
import org.junit.After
import org.junit.Before
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*

abstract class BaseMessagingTest {
    private var tempDir: Path? = null

    protected val newStore: MessagingStore
        get() = MessagingStore(
            InstrumentationRegistry.getInstrumentation().targetContext,
            dbPath = Paths.get(
                tempDir.toString(),
                UUID.randomUUID().toString(),
            ).toString()
        )

    @Before
    fun setupTempDir() {
        tempDir = Files.createTempDirectory("omtest")
    }

    @After
    fun deleteTempDir() {
        tempDir?.let {
            Files.walkFileTree(tempDir, object : FileVisitor<Path> {
                override fun preVisitDirectory(
                    dir: Path?,
                    attrs: BasicFileAttributes?
                ): FileVisitResult {
                    return FileVisitResult.CONTINUE;
                }

                override fun visitFile(file: Path?, attrs: BasicFileAttributes?): FileVisitResult {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                override fun visitFileFailed(file: Path?, exc: IOException?): FileVisitResult {
                    return FileVisitResult.CONTINUE;
                }

                override fun postVisitDirectory(dir: Path?, exc: IOException?): FileVisitResult {
                    return FileVisitResult.CONTINUE;
                }
            })
        }
    }
}