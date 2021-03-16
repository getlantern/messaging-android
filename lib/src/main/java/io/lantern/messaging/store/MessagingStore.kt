package io.lantern.messaging.store

import android.content.Context
import io.lantern.observablemodel.ObservableModel
import io.lantern.secrets.Secrets
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
import java.io.Closeable
import java.io.File

private val logger = KotlinLogging.logger {}

class MessagingStore(
    ctx: Context,
    dbPath: String = File(ctx.filesDir, "messaging.db").absolutePath,
    secretPrefsName: String = "secrets",
    masterKeyName: String = "messagingMasterKey",
    dbPasswordName: String = "messagingDbPassword",
    dbPasswordBytes: Int = 20
) : SignalProtocolStore, Closeable {
    internal val db: ObservableModel
    private val secrets: Secrets

    init {
        val secretsPreferences = ctx.getSharedPreferences(secretPrefsName, Context.MODE_PRIVATE)
        secrets = Secrets(masterKeyName, secretsPreferences)
        val dbPassword = secrets.get(dbPasswordName, dbPasswordBytes)!!
        db = ObservableModel.build(ctx, dbPath, dbPassword)
    }

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

    val deviceId: DeviceId
    get() {
        return db.mutate { tx ->
            val bytes = tx.get<ByteArray>(PATH_DEVICE_ID)
            if (bytes != null) {
                DeviceId(bytes)
            } else {
                val deviceId = DeviceId.random()
                tx.put(PATH_DEVICE_ID, deviceId.bytes)
                deviceId
            }
        }
    }

    fun generatePreKeys(count: Int): List<PreKeyRecord> {
        // TODO: handle the case of prekey ids rolling past MAX_INT
        return db.mutate { tx ->
            val nextId = tx.get(PATH_NEXT_ONE_TIME_PREKEY_ID) ?: 1
            val oneTimePreKeys = KeyHelper.generatePreKeys(nextId, count)
            oneTimePreKeys.forEach { preKey ->
                tx.put(oneTimePreKeyPath(preKey.id), preKey.serialize())
            }
            tx.put(PATH_NEXT_ONE_TIME_PREKEY_ID, nextId + count)
            oneTimePreKeys
        }
    }

    override fun storePreKey(preKeyId: Int, record: PreKeyRecord?) {
        throw AssertionError("storePreKey should never be called directly, please use generatePreKeys() to generate new keys")
    }

    override fun loadPreKey(preKeyId: Int): PreKeyRecord? {
        return db.get<ByteArray>(oneTimePreKeyPath(preKeyId))?.let { PreKeyRecord(it) }
            ?: throw InvalidKeyIdException("No one time preKey for id ${preKeyId}")
    }

    override fun containsPreKey(preKeyId: Int): Boolean {
        return db.contains(oneTimePreKeyPath(preKeyId))
    }

    override fun removePreKey(preKeyId: Int) {
        db.mutate { tx ->
            tx.delete(oneTimePreKeyPath(preKeyId))
        }
    }

    val nextSignedPreKey: SignedPreKeyRecord
        get() {
            // TODO: handle the case of signred prekey ids rolling past MAX_INT
            return db.mutate { tx ->
                val currentId = tx.get(PATH_CURRENT_SIGNED_PREKEY_ID) ?: 0
                val nextId = currentId + 1
                val signedPreKey = KeyHelper.generateSignedPreKey(identityKeyPair, nextId)
                tx.put(PATH_CURRENT_SIGNED_PREKEY_ID, nextId)
                tx.put(PATH_CURRENT_SIGNED_PREKEY, signedPreKey)
                tx.put(signedPreKeyPath(nextId), signedPreKey.serialize())
                signedPreKey
            }
        }

    override fun storeSignedPreKey(signedPreKeyId: Int, record: SignedPreKeyRecord?) {
        throw AssertionError("storeSignedPreKey should never be called directly, please use nextSignedPreKey to generate new keys")
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

    override fun containsSignedPreKey(signedPreKeyId: Int): Boolean {
        return db.contains(signedPreKeyPath(signedPreKeyId))
    }

    override fun removeSignedPreKey(signedPreKeyId: Int) {
        db.mutate { tx ->
            tx.delete(signedPreKeyPath(signedPreKeyId))
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

    override fun close() {
        db.close()
    }

    companion object {
        private const val PATH_SIGNAL_PROTOCOL_STORE = "/signalProtocolStore"

        private const val PATH_IDENTITY_KEY = "${PATH_SIGNAL_PROTOCOL_STORE}/identityKeyPair"
        private const val PATH_IDENTITY_KEY_PUBLIC = "${PATH_IDENTITY_KEY}/public"
        private const val PATH_IDENTITY_KEY_PRIVATE = "${PATH_IDENTITY_KEY}/private"

        private const val PATH_DEVICE_ID = "${PATH_SIGNAL_PROTOCOL_STORE}/deviceId"

        private const val PATH_PREKEYS = "${PATH_SIGNAL_PROTOCOL_STORE}/preKeys"
        private const val PATH_SIGNED_PREKEYS = "${PATH_PREKEYS}/signed"
        private const val PATH_CURRENT_SIGNED_PREKEY_ID = "${PATH_SIGNED_PREKEYS}/currentId"
        private const val PATH_CURRENT_SIGNED_PREKEY = "${PATH_SIGNED_PREKEYS}/current"
        private const val PATH_ALL_SIGNED_PREKEYS_BY_ID = "${PATH_SIGNED_PREKEYS}/all"
        private fun signedPreKeyPath(id: Int) = "${PATH_ALL_SIGNED_PREKEYS_BY_ID}/${id}"

        private const val PATH_ONE_TIME_PREKEYS = "${PATH_PREKEYS}/onetime"
        private const val PATH_NEXT_ONE_TIME_PREKEY_ID = "${PATH_ONE_TIME_PREKEYS}/nextId"
        private const val PATH_ALL_ONE_TIME_PREKEYS_BY_ID = "${PATH_ONE_TIME_PREKEYS}/all"
        private fun oneTimePreKeyPath(id: Int) = "${PATH_ALL_ONE_TIME_PREKEYS_BY_ID}/${id}"

        private const val PATH_ALL_SESSIONS_BY_ADDRESS = "${PATH_SIGNAL_PROTOCOL_STORE}/sessions"
        private fun sessionPath(address: SignalProtocolAddress) =
            "${PATH_ALL_SESSIONS_BY_ADDRESS}/${address}"

        private fun devicesForNamePath(name: String) = "${PATH_ALL_SESSIONS_BY_ADDRESS}/${name}:"
    }
}