package io.lantern.messaging

import java.security.InvalidKeyException
import java.security.SecureRandom
import org.whispersystems.curve25519.Curve25519
import org.whispersystems.libsignal.ecc.ECKeyPair
import org.whispersystems.libsignal.ecc.ECPrivateKey
import org.whispersystems.libsignal.ecc.ECPublicKey
import org.whispersystems.libsignal.kdf.HKDF

private val hkdf = HKDF.createFor(3)

/**
 * RecoveryKey is a seed for generating various keys used in the application. Keys are derived
 * using the HKDF defined in rfc5869.
 */
internal typealias RecoveryKey = ByteArray

/**
 * Generates an ECKeyPair with a derived secret from this RecoveryKey, using the given label.
 */
@Throws(InvalidKeyException::class)
internal fun RecoveryKey.keyPair(label: String): ECKeyPair {
    val derived = derive(label, 32)
    val curve25519 = Curve25519.getInstance(Curve25519.BEST)
    val privateKey = curve25519.generatePrivateKey(derived)
    derived.clear()
    val publicKey = curve25519.generatePublicKey(privateKey)
    return ECKeyPair(ECPublicKey(publicKey), ECPrivateKey(privateKey))
}

/**
 * Derives a secret from this RecoveryKey, of the specified length in bytes , using the given label
 */
internal fun RecoveryKey.derive(label: String, byteLength: Int): ByteArray =
    hkdf.deriveSecrets(this, label.toByteArray(Charsets.UTF_8), byteLength)

/**
 * Clears the contents of a ByteArray.
 */
fun ByteArray.clear() {
    for (i in 0 until size) {
        this[i] = 0
    }
}

/**
 * Generates a new random recovery key
 */
internal fun generateRecoveryKey(): RecoveryKey {
    val result = RecoveryKey(32)
    SecureRandom().nextBytes(result)
    return result
}
