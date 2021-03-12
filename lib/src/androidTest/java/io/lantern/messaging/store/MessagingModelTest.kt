package io.lantern.messaging.store

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertFalse
import org.junit.After
import org.junit.Before
import org.junit.runner.RunWith
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.InvalidKeyIdException
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.util.KeyHelper
import java.io.IOException
import java.nio.file.*
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.fail

@RunWith(AndroidJUnit4::class)
class MessagingModelTest {
    private var tempDir: Path? = null

    @Test
    fun testIdentityKeyPair() {
        newModel.use { model ->
            val kp1 = model.identityKeyPair
            val kp2 = model.identityKeyPair
            assertEquals(kp1.publicKey, kp2.publicKey)
            assertTrue(Arrays.equals(kp1.privateKey.bytes, kp2.privateKey.bytes))
        }
    }

    @Test
    fun testPreKeys() {
        newModel.use { model ->
            try {
                model.storePreKey(1, KeyHelper.generatePreKeys(0, 1)[0])
                fail("should not be allowed to directly store one time pre keys")
            } catch (e: AssertionError) {
                // expected
            }
            val somePks = model.generatePreKeys(2)
            assertEquals(2, somePks.size)
            val allPks = somePks + model.generatePreKeys(2)
            assertEquals(4, allPks.size)
            for (i in 1..4) {
                assertTrue(model.containsPreKey(i))
                val pk = allPks[i - 1].serialize()
                val loadedPk = model.loadPreKey(i)?.serialize()
                assertTrue(Arrays.equals(pk, loadedPk))
            }
            model.removePreKey(1)
            assertFalse(model.containsPreKey(1))
            try {
                model.loadPreKey(1)
                fail("loading non-existent preKey should throw an exception")
            } catch (e: InvalidKeyIdException) {
                // expected
            }
        }
    }

    @Test
    fun testSignedPreKeys() {
        newModel.use { model ->
            try {
                model.storeSignedPreKey(1, KeyHelper.generateSignedPreKey(model.identityKeyPair, 1))
                fail("should not be allowed to directly store signed pre keys")
            } catch (e: AssertionError) {
                // expected
            }
            val pk1 = model.nextSignedPreKey
            val pk2 = model.nextSignedPreKey
            assertTrue(model.containsSignedPreKey(1))
            assertTrue(Arrays.equals(pk1.serialize(), model.loadSignedPreKey(1)?.serialize()))
            assertEquals(2, model.loadSignedPreKeys().size)
            assertTrue(Arrays.equals(pk1.serialize(), model.loadSignedPreKeys()[0].serialize()))
            assertTrue(Arrays.equals(pk2.serialize(), model.loadSignedPreKeys()[1].serialize()))
            model.removeSignedPreKey(1)
            assertFalse(model.containsPreKey(1))
            assertEquals(1, model.loadSignedPreKeys().size)
            try {
                model.loadPreKey(1)
                fail("loading non-existent preKey should throw an exception")
            } catch (e: InvalidKeyIdException) {
                // expected
            }
        }
    }

    @Test
    fun testSessions() {
        newModel.use { model ->
            val address1 =
                SignalProtocolAddress(Curve.generateKeyPair().publicKey, DeviceId.random())
            val address2 = SignalProtocolAddress(address1.identityKey, DeviceId.random())
            val address3 =
                SignalProtocolAddress(Curve.generateKeyPair().publicKey, DeviceId.random())

            val session1 = model.loadSession(address1)
            assertEquals(0, session1.previousSessionStates.size)
            session1.archiveCurrentState()
            assertEquals(1, session1.previousSessionStates.size)
            model.storeSession(address1, session1)
            assertEquals(1, model.loadSession(address1).previousSessionStates.size)

            model.storeSession(address2, model.loadSession(address2))
            model.storeSession(address3, model.loadSession(address3))
            assertEquals(
                setOf(address1.deviceId, address2.deviceId),
                HashSet(model.getSubDeviceSessions(address1.identityKey.toString()))
            )

            assertTrue(model.containsSession(address1))
            model.deleteSession(address1)
            assertFalse(model.containsSession(address1))

            model.deleteAllSessions(address1.identityKey.toString())
            assertFalse(model.containsSession(address2))
            assertTrue(model.containsSession(address3))
        }
    }

    private val newModel: MessagingModel
        get() = MessagingModel(
            InstrumentationRegistry.getInstrumentation().targetContext,
            dbPath = Paths.get(
                tempDir.toString(),
                "testdb"
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