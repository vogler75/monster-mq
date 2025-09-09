package at.rocworks.stores

import at.rocworks.stores.cratedb.UserManagementStoreCrateDb
import at.rocworks.stores.mongodb.UserManagementStoreMongoDB
import at.rocworks.stores.postgres.UserManagementStorePostgres
import at.rocworks.stores.sqlite.UserManagementStoreSqlite
import io.vertx.core.json.JsonObject

object UserManagementStoreFactory {
    
    fun create(
        authStoreType: AuthStoreType,
        config: JsonObject
    ): IUserManagementStore {
        return when (authStoreType) {
            AuthStoreType.POSTGRES -> {
                val postgresConfig = config.getJsonObject("Postgres")
                UserManagementStorePostgres(
                    url = postgresConfig.getString("Url"),
                    username = postgresConfig.getString("User"),
                    password = postgresConfig.getString("Pass")
                )
            }
            AuthStoreType.SQLITE -> {
                val sqliteConfig = config.getJsonObject("SQLite")
                UserManagementStoreSqlite(
                    path = sqliteConfig.getString("Path")
                )
            }
            AuthStoreType.CRATEDB -> {
                val crateDbConfig = config.getJsonObject("CrateDB")
                UserManagementStoreCrateDb(
                    url = crateDbConfig.getString("Url"),
                    username = crateDbConfig.getString("User"),
                    password = crateDbConfig.getString("Pass")
                )
            }
            AuthStoreType.MONGODB -> {
                val mongoDbConfig = config.getJsonObject("MongoDB")
                UserManagementStoreMongoDB(
                    url = mongoDbConfig.getString("Url"),
                    database = mongoDbConfig.getString("Database")
                )
            }
        }
    }
}