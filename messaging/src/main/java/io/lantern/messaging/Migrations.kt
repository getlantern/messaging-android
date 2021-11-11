package io.lantern.messaging

import io.lantern.db.Transaction
import mu.KotlinLogging

const val versionField = "_migrationsDbVersion"

private val logger = KotlinLogging.logger {}

/**
 * Migrations applies database migrations to keep schema and data up-to-date.
 */
class Migrations(private val messaging: Messaging) {
    fun apply() {
        if (migrations.isEmpty()) {
            return
        }

        messaging.db.mutate { tx ->
            val currentVersion = tx.get(versionField) ?: 0
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
    private val migrations = sortedMapOf<Int, (tx: Transaction, startingVersion: Int) -> Unit> ()
}
