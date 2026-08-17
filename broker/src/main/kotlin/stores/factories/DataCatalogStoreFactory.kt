package at.rocworks.stores.factories

import at.rocworks.Const
import at.rocworks.stores.IDataCatalogStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import at.rocworks.stores.postgres.DataCatalogStorePostgres
import at.rocworks.stores.sqlite.DataCatalogStoreSQLite
import at.rocworks.stores.mongodb.DataCatalogStoreMongoDB

/**
 * Factory for creating DataCatalogStore implementations
 */
object DataCatalogStoreFactory {

    private var sharedInstance: IDataCatalogStore? = null

    fun setSharedInstance(store: IDataCatalogStore?) {
        sharedInstance = store
    }

    fun getSharedInstance(): IDataCatalogStore? = sharedInstance

    fun create(storeType: String?, config: JsonObject, vertx: Vertx): IDataCatalogStore? {
        sharedInstance?.let { return it }
        return when (storeType?.uppercase()) {
            "POSTGRES" -> {
                val postgresConfig = config.getJsonObject("Postgres")
                if (postgresConfig != null) {
                    val url = postgresConfig.getString("Url")
                    val user = postgresConfig.getString("User")
                    val pass = postgresConfig.getString("Pass")
                    val schema = postgresConfig.getString("Schema")
                    if (url != null && user != null && pass != null) {
                        DataCatalogStorePostgres(url, user, pass, schema)
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
                    val url = mongoConfig.getString("Url")
                    val database = mongoConfig.getString("Database")
                    if (url != null && database != null) {
                        DataCatalogStoreMongoDB(url, database)
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
                    val directory = sqliteConfig.getString("Path", Const.SQLITE_DEFAULT_PATH)
                    val dbPath = "$directory/monstermq.db"
                    DataCatalogStoreSQLite(vertx, dbPath)
                } else {
                    null
                }
            }

            else -> null
        }
    }
}
