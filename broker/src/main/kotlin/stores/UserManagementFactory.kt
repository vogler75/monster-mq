package at.rocworks.stores

import at.rocworks.stores.cratedb.UserManagementCrateDb
import at.rocworks.stores.mongodb.UserManagementMongoDB
import at.rocworks.stores.postgres.UserManagementPostgres
import at.rocworks.stores.sqlite.UserManagementSqlite
import io.vertx.core.json.JsonObject

object UserManagementFactory {
    
    fun create(
        authStoreType: AuthStoreType,
        config: JsonObject
    ): IUserManagement {
        return when (authStoreType) {
            AuthStoreType.POSTGRES -> {
                val postgresConfig = config.getJsonObject("Postgres")
                UserManagementPostgres(
                    url = postgresConfig.getString("Url"),
                    username = postgresConfig.getString("User"),
                    password = postgresConfig.getString("Pass")
                )
            }
            AuthStoreType.SQLITE -> {
                val sqliteConfig = config.getJsonObject("SQLite")
                UserManagementSqlite(
                    path = sqliteConfig.getString("Path")
                )
            }
            AuthStoreType.CRATEDB -> {
                val crateDbConfig = config.getJsonObject("CrateDB")
                UserManagementCrateDb(
                    url = crateDbConfig.getString("Url"),
                    username = crateDbConfig.getString("User"),
                    password = crateDbConfig.getString("Pass")
                )
            }
            AuthStoreType.MONGODB -> {
                val mongoDbConfig = config.getJsonObject("MongoDB")
                UserManagementMongoDB(
                    url = mongoDbConfig.getString("Url"),
                    database = mongoDbConfig.getString("Database")
                )
            }
        }
    }
}