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
                    apply(tx, currentVersion)
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
    private val migrations = sortedMapOf<Int, (tx: Transaction, startingVersion: Int) -> Unit> (
        1 to { tx, _ ->
            // Prior to this, we did not allow messages from unrecognized contacts, nor did we allow
            // explicit verification of contacts, so we mark all existing contacts UNVERIFIED.
            tx.list<Model.Contact>(Schema.PATH_CONTACTS.path("%")).forEach { contact ->
                tx.put(
                    contact.path,
                    contact.value.toBuilder()
                        .setVerificationLevel(Model.VerificationLevel.UNVERIFIED).build()
                )

                // decrypt any "spam" we received from this Contact prior to adding them
                tx.list<ByteArray>(Schema.PATH_SPAM.path(contact.value.contactId.id, "%"))
                    .forEach { (spamPath, unidentifiedSenderMessage) ->
                        messaging.cryptoWorker.doDecryptAndStore(unidentifiedSenderMessage)
                        tx.delete(spamPath)
                    }
            }
        },

        2 to { tx, _ ->
            // set new constrainedVerificationLevel field on all introductions
            tx.listDetails<Model.StoredMessage>(
                Schema.PATH_INTRODUCTIONS_BY_FROM.path('%')
            ).forEach { msg ->
                val sender = tx.get<Model.Contact>(msg.value.senderId.directContactPath)
                val msgBuilder = msg.value.toBuilder()
                msgBuilder.introduction =
                    msg.value.introduction.toBuilder()
                        .setConstrainedVerificationLevelValue(
                            if (sender == null) {
                                Model.VerificationLevel.UNACCEPTED.number
                            } else {
                                msg.value.introduction.verificationLevelValue.coerceAtMost(
                                    sender.verificationLevelValue
                                )
                            }
                        ).build()
                tx.put(msg.value.dbPath, msgBuilder.build())
            }

            tx.listDetails<Model.StoredMessage>(
                Schema.PATH_INTRODUCTIONS_BY_TO.path('%')
            ).groupBy { it.value.introduction.to }.keys.forEach {
                messaging.updateBestIntroduction(tx, it)
            }
        },

        3 to { tx, _ ->
            tx.get<Model.Contact>(Schema.PATH_ME)?.let { me ->
                tx.put(Schema.PATH_ME, me.toBuilder().setIsMe(true).build())
            }
        }
    )
}
