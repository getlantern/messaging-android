package io.lantern.messaging.store

import androidx.test.ext.junit.runners.AndroidJUnit4
import io.lantern.messaging.BaseMessagingTest
import java.util.Arrays
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.fail
import org.junit.runner.RunWith
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.InvalidKeyIdException
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.ecc.ECPrivateKey
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.util.KeyHelper

@RunWith(AndroidJUnit4::class)
class MessagingProtocolStoreTest : BaseMessagingTest() {
    @Test
    fun testIdentityKeyPair() {
        newDB.use { db ->
            val ms = newStore(db = db)
            val kp1 = ms.identityKeyPair
            val kp2 = ms.identityKeyPair
            assertEquals(kp1.publicKey, kp2.publicKey)
            assertTrue(Arrays.equals(kp1.privateKey.bytes, kp2.privateKey.bytes))
            assertEquals(
                kp1.publicKey,
                ECPublicKey(
                    db.withSchema("messaging_protocol_store")
                        .get<ByteArray>(MessagingProtocolStore.PATH_IDENTITY_KEY_PUBLIC)
                )
            )
            assertTrue(
                Arrays.equals(
                    kp1.privateKey.bytes,
                    ECPrivateKey(
                        db.withSchema("messaging_protocol_store")
                            .get(MessagingProtocolStore.PATH_IDENTITY_KEY_PRIVATE)
                    ).bytes
                )
            )
        }
    }

    @Test
    fun testPreKeys() {
        newDB.use { db ->
            val ms = newStore(db)
            try {
                ms.storePreKey(1, KeyHelper.generatePreKeys(0, 1)[0])
                fail("should not be allowed to directly store one time pre keys")
            } catch (e: AssertionError) {
                // expected
            }
            val somePks = ms.generatePreKeys(2)
            assertEquals(2, somePks.size)
            val allPks = somePks + ms.generatePreKeys(2)
            assertEquals(4, allPks.size)
            for (i in 1..4) {
                assertTrue(ms.containsPreKey(i))
                val pk = allPks[i - 1].serialize()
                val loadedPk = ms.loadPreKey(i).serialize()
                assertTrue(Arrays.equals(pk, loadedPk))
            }
            ms.removePreKey(1)
            assertFalse(ms.containsPreKey(1))
            try {
                ms.loadPreKey(1)
                fail("loading non-existent preKey should throw an exception")
            } catch (e: InvalidKeyIdException) {
                // expected
            }
        }
    }

    @Test
    fun testSignedPreKeys() {
        newDB.use { db ->
            val ms = newStore(db)
            try {
                ms.storeSignedPreKey(1, KeyHelper.generateSignedPreKey(ms.identityKeyPair, 1))
                fail("should not be allowed to directly store signed pre keys")
            } catch (e: AssertionError) {
                // expected
            }
            val pk1 = ms.nextSignedPreKey
            val pk2 = ms.nextSignedPreKey
            assertTrue(ms.containsSignedPreKey(1))
            assertTrue(Arrays.equals(pk1.serialize(), ms.loadSignedPreKey(1).serialize()))
            assertEquals(2, ms.loadSignedPreKeys().size)
            assertTrue(Arrays.equals(pk1.serialize(), ms.loadSignedPreKeys()[0].serialize()))
            assertTrue(Arrays.equals(pk2.serialize(), ms.loadSignedPreKeys()[1].serialize()))
            ms.removeSignedPreKey(1)
            assertFalse(ms.containsPreKey(1))
            assertEquals(1, ms.loadSignedPreKeys().size)
            try {
                ms.loadPreKey(1)
                fail("loading non-existent preKey should throw an exception")
            } catch (e: InvalidKeyIdException) {
                // expected
            }
        }
    }

    @Test
    fun testSessions() {
        newDB.use { db ->
            val ms = newStore(db)
            val address1 =
                SignalProtocolAddress(Curve.generateKeyPair().publicKey, DeviceId.random())
            val address2 = SignalProtocolAddress(address1.identityKey, DeviceId.random())
            val address3 =
                SignalProtocolAddress(Curve.generateKeyPair().publicKey, DeviceId.random())

            val session1 = ms.loadSession(address1)
            assertEquals(0, session1.previousSessionStates.size)
            session1.archiveCurrentState()
            assertEquals(1, session1.previousSessionStates.size)
            ms.storeSession(address1, session1)
            assertEquals(1, ms.loadSession(address1).previousSessionStates.size)

            ms.storeSession(address2, ms.loadSession(address2))
            ms.storeSession(address3, ms.loadSession(address3))
            assertEquals(
                setOf(address1.deviceId, address2.deviceId),
                HashSet(ms.getSubDeviceSessions(address1.identityKey.toString()))
            )

            assertTrue(ms.containsSession(address1))
            ms.deleteSession(address1)
            assertFalse(ms.containsSession(address1))

            ms.deleteAllSessions(address1.identityKey.toString())
            assertFalse(ms.containsSession(address2))
            assertTrue(ms.containsSession(address3))
        }
    }
}
