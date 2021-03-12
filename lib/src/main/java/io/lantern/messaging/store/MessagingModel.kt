package io.lantern.messaging.store

import io.lantern.observablemodel.ObservableModel
import mu.KotlinLogging
import org.whispersystems.libsignal.DeviceId
import org.whispersystems.libsignal.InvalidKeyIdException
import org.whispersystems.libsignal.SignalProtocolAddress
import org.whispersystems.libsignal.ecc.Curve
import org.whispersystems.libsignal.ecc.ECKeyPair
import org.whispersystems.libsignal.ecc.ECPrivateKey
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.state.PreKeyRecord
import org.whispersystems.libsignal.state.SessionRecord
import org.whispersystems.libsignal.state.SignalProtocolStore
import org.whispersystems.libsignal.state.SignedPreKeyRecord
import org.whispersystems.libsignal.util.KeyHelper

private val logger = KotlinLogging.logger {}

class MessagingModel(private val db: ObservableModel) : SignalProtocolStore {
    override fun getIdentityKeyPair(): ECKeyPair {
        return db.mutate { tx ->
            val public = db.get<ByteArray>(PATH_IDENTITY_KEY_PUBLIC)
            val private = db.get<ByteArray>(PATH_IDENTITY_KEY_PRIVATE)

            if (public != null && private != null) {
                ECKeyPair(ECPublicKey(public), ECPrivateKey(private))
            } else {
                val keyPair = Curve.generateKeyPair()
                tx.put(PATH_IDENTITY_KEY_PUBLIC, keyPair.publicKey.bytes)
                tx.put(PATH_IDENTITY_KEY_PRIVATE, keyPair.privateKey.bytes)
                keyPair
            }
        }
    }

    override fun loadPreKey(preKeyId: Int): PreKeyRecord? {
        return db.get<ByteArray>(oneTimePreKeyPath(preKeyId))?.let { PreKeyRecord(it) }
            ?: throw InvalidKeyIdException("No one time preKey for id ${preKeyId}")
    }

    override fun storePreKey(preKeyId: Int, record: PreKeyRecord?) {
        db.mutate { tx ->
            tx.put(oneTimePreKeyPath(preKeyId), record?.serialize())
        }
    }

    override fun containsPreKey(preKeyId: Int): Boolean {
        return db.contains(oneTimePreKeyPath(preKeyId))
    }

    override fun removePreKey(preKeyId: Int) {
        db.mutate { tx ->
            tx.delete(oneTimePreKeyPath(preKeyId))
        }
    }

    override fun loadSignedPreKey(signedPreKeyId: Int): SignedPreKeyRecord {
        return db.get<ByteArray>(signedPreKeyPath(signedPreKeyId))?.let { SignedPreKeyRecord(it) }
            ?: throw InvalidKeyIdException("No signed preKey for id ${signedPreKeyId}")
    }

    override fun loadSignedPreKeys(): MutableList<SignedPreKeyRecord> {
        return ArrayList(
            db.list<ByteArray>("${PATH_ALL_SIGNED_PREKEYS_BY_ID}/%")
                .map { SignedPreKeyRecord(it.value) })
    }

    override fun storeSignedPreKey(signedPreKeyId: Int, record: SignedPreKeyRecord?) {
        throw AssertionError("storeSignedPreKey should never be called directly, please use nextSignedPreKey to generate new keys")
    }

    override fun containsSignedPreKey(signedPreKeyId: Int): Boolean {
        return db.contains(signedPreKeyPath(signedPreKeyId))
    }

    override fun removeSignedPreKey(signedPreKeyId: Int) {
        db.mutate { tx ->
            tx.delete(signedPreKeyPath(signedPreKeyId))
        }
    }

    val nextSignedPreKey: SignedPreKeyRecord
        get() {
            return db.mutate { tx ->
                val currentPreKeyId = tx.get(PATH_CURRENT_SIGNED_PREKEY_ID) ?: 0
                val nextPreKeyId = currentPreKeyId + 1
                val signedPreKey = KeyHelper.generateSignedPreKey(identityKeyPair, nextPreKeyId)
                tx.put(PATH_CURRENT_SIGNED_PREKEY_ID, nextPreKeyId)
                tx.put(PATH_CURRENT_SIGNED_PREKEY, signedPreKey)
                tx.put(signedPreKeyPath(nextPreKeyId), signedPreKey.serialize())
                signedPreKey
            }
        }

    override fun loadSession(address: SignalProtocolAddress): SessionRecord {
        var path = sessionPath(address)
        return db.mutate { tx ->
            var bytes = tx.get<ByteArray>(path)
            if (bytes != null) {
                SessionRecord(bytes)
            } else {
                val session = SessionRecord()
                tx.put(path, session.serialize())
                session
            }
        }
    }

    override fun getSubDeviceSessions(name: String): MutableList<DeviceId> {
        return ArrayList(
            db.listPaths("${devicesForNamePath(name)}%")
                .map { SignalProtocolAddress(it.split("/").last()).deviceId })
    }

    override fun storeSession(address: SignalProtocolAddress, record: SessionRecord?) {
        db.mutate { tx -> tx.put(sessionPath(address), record?.serialize()) }
    }

    override fun containsSession(address: SignalProtocolAddress): Boolean {
        return db.contains(sessionPath(address))
    }

    override fun deleteSession(address: SignalProtocolAddress) {
        db.mutate { tx -> tx.delete(sessionPath(address)) }
    }

    override fun deleteAllSessions(name: String) {
        db.mutate { tx ->
            db.listRaw<ByteArray>("${devicesForNamePath(name)}%").forEach { tx.delete(it.path) }
        }
    }

    companion object {
        private const val PATH_SIGNAL_PROTOCOL_STORE = "/signalProtocolStore"

        private const val PATH_IDENTITY_KEY = "${PATH_SIGNAL_PROTOCOL_STORE}/identityKeyPair"
        private const val PATH_IDENTITY_KEY_PUBLIC = "${PATH_IDENTITY_KEY}/public"
        private const val PATH_IDENTITY_KEY_PRIVATE = "${PATH_IDENTITY_KEY}/private"

        private const val PATH_PREKEYS = "${PATH_SIGNAL_PROTOCOL_STORE}/preKeys"
        private const val PATH_SIGNED_PREKEYS = "${PATH_PREKEYS}/signed"
        private const val PATH_CURRENT_SIGNED_PREKEY_ID = "${PATH_SIGNED_PREKEYS}/currentId"
        private const val PATH_CURRENT_SIGNED_PREKEY = "${PATH_SIGNED_PREKEYS}/current"
        private const val PATH_ALL_SIGNED_PREKEYS_BY_ID = "${PATH_SIGNED_PREKEYS}/all"
        private fun signedPreKeyPath(id: Int) = "${PATH_ALL_SIGNED_PREKEYS_BY_ID}/${id}"

        private const val PATH_ONETIME_PREKEYS = "${PATH_PREKEYS}/onetime"
        private const val PATH_ALL_ONETIME_PREKEYS_BY_ID = "${PATH_ONETIME_PREKEYS}/all"
        private fun oneTimePreKeyPath(id: Int) = "${PATH_ALL_ONETIME_PREKEYS_BY_ID}/${id}"

        private const val PATH_ALL_SESSIONS_BY_ADDRESS = "${PATH_SIGNAL_PROTOCOL_STORE}/sessions"
        private fun sessionPath(address: SignalProtocolAddress) =
            "${PATH_ALL_SESSIONS_BY_ADDRESS}/${address.toString()}"

        private fun devicesForNamePath(name: String) = "${PATH_ALL_SESSIONS_BY_ADDRESS}/${name}:"
    }


}