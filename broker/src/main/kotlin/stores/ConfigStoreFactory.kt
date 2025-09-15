package at.rocworks.stores

import at.rocworks.stores.postgres.ConfigStorePostgres
import at.rocworks.stores.mongodb.ConfigStoreMongoDB
import at.rocworks.stores.cratedb.ConfigStoreCrateDB
import at.rocworks.stores.sqlite.ConfigStoreSQLite
import io.vertx.core.json.JsonObject

object ConfigStoreFactory {
    fun createConfigStore(config: JsonObject, storeType: String?): IConfigStore? {
        return when (storeType?.uppercase()) {
            "POSTGRES" -> {
                val postgresConfig = config.getJsonObject("Postgres")
                if (postgresConfig != null) {
                    val url = postgresConfig.getString("Url")
                    val user = postgresConfig.getString("User")
                    val pass = postgresConfig.getString("Pass")
                    if (url != null && user != null && pass != null) {
                        ConfigStorePostgres(url, user, pass)
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
                        ConfigStoreMongoDB(connectionString, databaseName)
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
                        ConfigStoreCrateDB(url, username, password)
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
                    val dbPath = sqliteConfig.getString("DbPath")
                    if (dbPath != null) {
                        ConfigStoreSQLite(dbPath)
                    } else {
                        null
                    }
                } else {
                    null
                }
            }
            else -> null
        }
    }
}