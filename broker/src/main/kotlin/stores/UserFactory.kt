package at.rocworks.stores

import at.rocworks.stores.cratedb.UserStoreCrateDb
import at.rocworks.stores.mongodb.UserStoreMongoDB
import at.rocworks.stores.postgres.UserStorePostgres
import at.rocworks.stores.sqlite.UserStoreSqlite
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

object UserFactory {

    fun create(
        storeType: StoreType,
        config: JsonObject,
        vertx: Vertx
    ): IUserStore {
        return when (storeType) {
            StoreType.POSTGRES -> {
                val postgresConfig = config.getJsonObject("Postgres")
                UserStorePostgres(
                    url = postgresConfig.getString("Url"),
                    username = postgresConfig.getString("User"),
                    password = postgresConfig.getString("Pass"),
                    vertx = vertx
                )
            }
            StoreType.SQLITE -> {
                val sqliteConfig = config.getJsonObject("SQLite")
                UserStoreSqlite(
                    path = sqliteConfig.getString("Path"),
                    vertx = vertx
                )
            }
            StoreType.CRATEDB -> {
                val crateDbConfig = config.getJsonObject("CrateDB")
                UserStoreCrateDb(
                    url = crateDbConfig.getString("Url"),
                    username = crateDbConfig.getString("User"),
                    password = crateDbConfig.getString("Pass"),
                    vertx = vertx
                )
            }
            StoreType.MONGODB -> {
                val mongoDbConfig = config.getJsonObject("MongoDB")
                UserStoreMongoDB(
                    url = mongoDbConfig.getString("Url"),
                    database = mongoDbConfig.getString("Database"),
                    vertx = vertx
                )
            }
        }
    }
}