package io.lantern.messaging

import io.lantern.db.Transaction
import mu.KotlinLogging

const val versionField = "_migrationsDbVersion"

private val logger = KotlinLogging.logger {}

private class Migration(val version: Int, val apply: () -> Unit) : Comparable<Migration> {
    override fun compareTo(other: Migration): Int = this.version - other.version
}

/**
 * Migrations applies database migrations to keep schema and data up-to-date.
 */
class Migrations(private val messaging: Messaging) {
    fun apply() {
        messaging.db.mutate { tx ->
            val currentVersion = tx.get<Int>(versionField) ?: 0
            logger.debug("current database version is $currentVersion")
            migrations.forEach { (version, apply) ->
                if (version > currentVersion) {
                    logger.debug("applying migration to version $version")
                    apply(tx)
                }
            }
            val newVersion = migrations.lastKey()
            tx.put(versionField, newVersion)
            logger.debug("migrated database to version $newVersion")
        }
    }

    // Define migrations here. Whenever you update the messaging-android library with changes that
    // are incompatible with the prior version, add a new migration function in here with a new
    // version number that's higher than the prior migration.
    private val migrations = sortedMapOf<Int, (tx: Transaction) -> Unit> (
        1 to { tx ->
            // Prior to this, we did not allow messages from unrecognized contacts, nor did we allow
            // explicit verification of contacts, so we mark all existing contacts UNVERIFIED.
            tx.list<Model.Contact>(Schema.PATH_CONTACTS.path("%")).forEach { contact ->
                tx.put(
                    contact.path,
                    contact.value.toBuilder()
                        .setVerificationLevel(Model.VerificationLevel.UNVERIFIED).build()
                )

                // decrypt any "spam" we received from this Contact prior to adding them
                tx.list<ByteArray>(contact.value.spamQuery)
                    .forEach { (spamPath, unidentifiedSenderMessage) ->
                        messaging.cryptoWorker.doDecryptAndStore(unidentifiedSenderMessage)
                        tx.delete(spamPath)
                    }
            }
        },

        2 to { tx ->
            // calculate new hue field for all contacts
            tx.list<Model.Contact>(Schema.PATH_CONTACTS.path("%")).forEach { contact ->
                tx.put(
                    contact.path,
                    contact.value.toBuilder()
                        .setHue(contact.value.contactId.hue).build()
                )
            }
        }
    )
}
