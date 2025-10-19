package at.rocworks.stores

import at.rocworks.Const
import at.rocworks.stores.postgres.MetricsStorePostgres
import at.rocworks.stores.cratedb.MetricsStoreCrateDB
import at.rocworks.stores.mongodb.MetricsStoreMongoDB
import at.rocworks.stores.sqlite.MetricsStoreSQLite
import io.vertx.core.json.JsonObject

object MetricsStoreFactory {

    fun create(
        metricsStoreType: MetricsStoreType,
        config: JsonObject,
        storeName: String = "metrics"
    ): IMetricsStoreAsync {
        return when (metricsStoreType) {
            MetricsStoreType.POSTGRES -> {
                val postgresConfig = config.getJsonObject("Postgres")
                    ?: throw IllegalArgumentException("PostgreSQL configuration not found")
                MetricsStorePostgres(
                    name = storeName,
                    url = postgresConfig.getString("Url"),
                    username = postgresConfig.getString("User"),
                    password = postgresConfig.getString("Pass"),
                    schema = postgresConfig.getString("Schema")
                )
            }
            MetricsStoreType.CRATEDB -> {
                val crateDbConfig = config.getJsonObject("CrateDB")
                    ?: throw IllegalArgumentException("CrateDB configuration not found")
                MetricsStoreCrateDB(
                    name = storeName,
                    url = crateDbConfig.getString("Url"),
                    username = crateDbConfig.getString("User"),
                    password = crateDbConfig.getString("Pass")
                )
            }
            MetricsStoreType.MONGODB -> {
                val mongoDbConfig = config.getJsonObject("MongoDB")
                    ?: throw IllegalArgumentException("MongoDB configuration not found")
                MetricsStoreMongoDB(
                    name = storeName,
                    connectionString = mongoDbConfig.getString("Url"),
                    databaseName = mongoDbConfig.getString("Database")
                )
            }
            MetricsStoreType.SQLITE -> {
                val sqliteConfig = config.getJsonObject("SQLite")
                    ?: throw IllegalArgumentException("SQLite configuration not found")
                val directory = sqliteConfig.getString("Path", Const.SQLITE_DEFAULT_PATH)
                val dbPath = "$directory/monstermq.db"
                MetricsStoreSQLite(
                    name = storeName,
                    dbPath = dbPath
                )
            }
        }
    }

    fun createFromConfig(
        config: JsonObject,
        storeName: String = "metrics"
    ): IMetricsStoreAsync? {
        val metricsConfig = config.getJsonObject("MetricsStore", JsonObject())
        val typeString = metricsConfig.getString("Type")

        if (typeString.isNullOrEmpty()) {
            // Auto-detect based on available database configurations
            return when {
                config.containsKey("Postgres") -> {
                    create(MetricsStoreType.POSTGRES, config, storeName)
                }
                config.containsKey("CrateDB") -> {
                    create(MetricsStoreType.CRATEDB, config, storeName)
                }
                config.containsKey("MongoDB") -> {
                    create(MetricsStoreType.MONGODB, config, storeName)
                }
                config.containsKey("SQLite") -> {
                    create(MetricsStoreType.SQLITE, config, storeName)
                }
                else -> null
            }
        }

        val type = try {
            MetricsStoreType.valueOf(typeString.uppercase())
        } catch (e: IllegalArgumentException) {
            throw IllegalArgumentException("Invalid metrics store type: $typeString. Valid types: ${MetricsStoreType.values().joinToString()}")
        }

        return create(type, config, storeName)
    }
}