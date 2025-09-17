package at.rocworks.stores

import at.rocworks.stores.cratedb.UserStoreCrateDb
import at.rocworks.stores.mongodb.UserStoreMongoDB
import at.rocworks.stores.postgres.UserStorePostgres
import at.rocworks.stores.sqlite.UserStoreSqlite
import io.vertx.core.json.JsonObject

object UserFactory {
    
    fun create(
        authStoreType: AuthStoreType,
        config: JsonObject
    ): IUserStore {
        return when (authStoreType) {
            AuthStoreType.POSTGRES -> {
                val postgresConfig = config.getJsonObject("Postgres")
                UserStorePostgres(
                    url = postgresConfig.getString("Url"),
                    username = postgresConfig.getString("User"),
                    password = postgresConfig.getString("Pass")
                )
            }
            AuthStoreType.SQLITE -> {
                val sqliteConfig = config.getJsonObject("SQLite")
                UserStoreSqlite(
                    path = sqliteConfig.getString("Path")
                )
            }
            AuthStoreType.CRATEDB -> {
                val crateDbConfig = config.getJsonObject("CrateDB")
                UserStoreCrateDb(
                    url = crateDbConfig.getString("Url"),
                    username = crateDbConfig.getString("User"),
                    password = crateDbConfig.getString("Pass")
                )
            }
            AuthStoreType.MONGODB -> {
                val mongoDbConfig = config.getJsonObject("MongoDB")
                UserStoreMongoDB(
                    url = mongoDbConfig.getString("Url"),
                    database = mongoDbConfig.getString("Database")
                )
            }
        }
    }
}