package at.rocworks.stores

import at.rocworks.stores.postgres.ArchiveConfigStorePostgres
import at.rocworks.stores.mongodb.ArchiveConfigStoreMongoDB
import at.rocworks.stores.cratedb.ArchiveConfigStoreCrateDB
import at.rocworks.stores.sqlite.ArchiveConfigStoreSQLite
import io.vertx.core.json.JsonObject

object ArchiveConfigStoreFactory {
    fun createConfigStore(config: JsonObject, storeType: String?): IArchiveConfigStore? {
        return when (storeType?.uppercase()) {
            "POSTGRES" -> {
                val postgresConfig = config.getJsonObject("Postgres")
                if (postgresConfig != null) {
                    val url = postgresConfig.getString("Url")
                    val user = postgresConfig.getString("User")
                    val pass = postgresConfig.getString("Pass")
                    if (url != null && user != null && pass != null) {
                        ArchiveConfigStorePostgres(url, user, pass)
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
                    val databaseName = mongoConfig.getString("Database")
                    if (connectionString != null && databaseName != null) {
                        ArchiveConfigStoreMongoDB(connectionString, databaseName)
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
                    val username = crateConfig.getString("Username")
                    val password = crateConfig.getString("Password")
                    if (url != null && username != null && password != null) {
                        ArchiveConfigStoreCrateDB(url, username, password)
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
                    val directory = sqliteConfig.getString("Path", ".")
                    val dbPath = "$directory/monstermq.db"
                    ArchiveConfigStoreSQLite(dbPath)
                } else {
                    null
                }
            }
            else -> null
        }
    }
}