package at.rocworks.stores

import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import at.rocworks.stores.postgres.DeviceConfigStorePostgres
import at.rocworks.stores.sqlite.DeviceConfigStoreSQLite

/**
 * Factory for creating DeviceConfigStore implementations
 */
object DeviceConfigStoreFactory {

    /**
     * Create a DeviceConfigStore based on the config store type
     * (reuse the same database configuration as the config store)
     */
    fun create(storeType: String?, config: JsonObject, vertx: Vertx): IDeviceConfigStore? {
        return when (storeType?.uppercase()) {
            "POSTGRES" -> {
                val postgresConfig = config.getJsonObject("Postgres")
                if (postgresConfig != null) {
                    val url = postgresConfig.getString("Url")
                    val user = postgresConfig.getString("User")
                    val pass = postgresConfig.getString("Pass")
                    if (url != null && user != null && pass != null) {
                        DeviceConfigStorePostgres(url, user, pass)
                    } else {
                        null
                    }
                } else {
                    null
                }
            }

            "CRATEDB" -> {
                val crateConfig = config.getJsonObject("CrateDB")
                if (crateConfig != null) {
                    val url = crateConfig.getString("Url")
                    val user = crateConfig.getString("User")
                    val pass = crateConfig.getString("Pass")
                    if (url != null && user != null && pass != null) {
                        // TODO: Implement DeviceConfigStoreCrateDB
                        throw NotImplementedError("CrateDB device config store not yet implemented")
                    } else {
                        null
                    }
                } else {
                    null
                }
            }

            "MONGODB" -> {
                val mongoConfig = config.getJsonObject("MongoDB")
                if (mongoConfig != null) {
                    val connectionString = mongoConfig.getString("ConnectionString")
                    val database = mongoConfig.getString("Database")
                    if (connectionString != null && database != null) {
                        // TODO: Implement DeviceConfigStoreMongoDB
                        throw NotImplementedError("MongoDB device config store not yet implemented")
                    } else {
                        null
                    }
                } else {
                    null
                }
            }

            "SQLITE" -> {
                val sqliteConfig = config.getJsonObject("SQLite")
                if (sqliteConfig != null) {
                    // Use main database file for device configs
                    val directory = sqliteConfig.getString("Path", ".")
                    val dbPath = "$directory/monstermq.db"
                    DeviceConfigStoreSQLite(vertx, dbPath)
                } else {
                    null
                }
            }

            else -> null
        }
    }
}