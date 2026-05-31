package at.rocworks.stores

import at.rocworks.Monster
import at.rocworks.Utils
import at.rocworks.stores.sqlite.KafkaQueueStoreSQLite
import at.rocworks.stores.postgres.KafkaQueueStorePostgres
import at.rocworks.stores.mongodb.KafkaQueueStoreMongoDB
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

object KafkaQueueStoreFactory {
    private val logger = Utils.getLogger(this::class.java)

    fun create(vertx: Vertx, configJson: JsonObject, tableNameSuffix: String = ""): Future<IKafkaQueueStore> {
        val storeType = getStoreType(configJson)
        val storeTypeUpper = storeType.uppercase()
        
        logger.info("Initializing Kafka Queue Store of type/connection: $storeType (suffix: $tableNameSuffix)")

        val archiveHandler = Monster.getArchiveHandler()
        val configStore = archiveHandler?.getConfigStore()
        
        val connectionFuture = if (configStore != null && storeType.isNotBlank() && 
            storeTypeUpper != "MEMORY" && storeTypeUpper != "SQLITE" && 
            storeTypeUpper != "POSTGRES" && storeTypeUpper != "MONGODB" && storeTypeUpper != "CRATEDB") {
            configStore.getDatabaseConnection(storeType)
        } else {
            Future.succeededFuture(null)
        }

        return connectionFuture.compose { connection ->
            val store = if (connection != null) {
                logger.info("Using dynamic database connection '${connection.name}' of type '${connection.type}' for Kafka queue store")
                when (connection.type) {
                    DatabaseConnectionType.POSTGRES -> {
                        val url = connection.url
                        val user = connection.username ?: "postgres"
                        val pass = connection.password ?: "postgres"
                        val schema = connection.schema ?: "public"
                        KafkaQueueStorePostgres(url, user, pass, schema, tableNameSuffix)
                    }
                    DatabaseConnectionType.MONGODB -> {
                        val url = connection.url
                        val database = connection.database ?: "monstermq"
                        KafkaQueueStoreMongoDB(url, database, tableNameSuffix)
                    }
                    DatabaseConnectionType.SQLITE -> {
                        val dbPath = connection.url
                        KafkaQueueStoreSQLite(dbPath, tableNameSuffix)
                    }
                }
            } else {
                when (storeTypeUpper) {
                    "POSTGRES" -> {
                        val pg = configJson.getJsonObject("Postgres", JsonObject())
                        val url = pg.getString("Url", "jdbc:postgresql://localhost:5432/monstermq")
                        val user = pg.getString("User", "postgres")
                        val pass = pg.getString("Pass") ?: pg.getString("Password", "postgres")
                        val schema = pg.getString("Schema", "public")
                        KafkaQueueStorePostgres(url, user, pass, schema, tableNameSuffix)
                    }
                    "MONGODB" -> {
                        val mongo = configJson.getJsonObject("MongoDB", JsonObject())
                        val url = mongo.getString("Url", "mongodb://localhost:27017")
                        val database = mongo.getString("Database", "monstermq")
                        KafkaQueueStoreMongoDB(url, database, tableNameSuffix)
                    }
                    "SQLITE", "MEMORY" -> {
                        val sqlite = configJson.getJsonObject("SQLite", JsonObject())
                        val dbPath = if (storeTypeUpper == "MEMORY") {
                            "file::memory:?cache=shared"
                        } else {
                            sqlite.getString("DatabasePath", "monstermq.db")
                        }
                        KafkaQueueStoreSQLite(dbPath, tableNameSuffix)
                    }
                    else -> {
                        logger.warning("Unsupported store type for Kafka queue: $storeType, falling back to SQLite Memory")
                        KafkaQueueStoreSQLite("file::memory:?cache=shared", tableNameSuffix)
                    }
                }
            }

            vertx.deployVerticle(store).map { store as IKafkaQueueStore }
        }
    }

    private fun getStoreType(configJson: JsonObject): String {
        // 1. Check if there is an instance-specific override in the KafkaServer block
        val kafkaServer = configJson.getJsonObject("KafkaServer")
        if (kafkaServer != null) {
            val deviceStoreType = kafkaServer.getString("storeType")
            if (deviceStoreType != null && deviceStoreType.isNotBlank()) {
                return deviceStoreType
            }
        }

        // 2. Fallback to authoritative global default settings matching MonsterMQ
        val explicitQueue = configJson.getString("QueueStoreType")
        if (explicitQueue != null) {
            val qType = explicitQueue.uppercase()
            if (qType.contains("POSTGRES")) return "POSTGRES"
            if (qType.contains("MONGO")) return "MONGODB"
            if (qType.contains("SQLITE")) return "SQLITE"
            if (qType.contains("MEMORY")) return "MEMORY"
        }

        val defaultStore = configJson.getString("DefaultStoreType")
        if (defaultStore != null) {
            val dType = defaultStore.uppercase()
            if (dType.contains("POSTGRES")) return "POSTGRES"
            if (dType.contains("MONGO")) return "MONGODB"
            if (dType.contains("SQLITE")) return "SQLITE"
            if (dType.contains("MEMORY")) return "MEMORY"
        }

        val sessionStore = configJson.getString("SessionStoreType")
        if (sessionStore != null && sessionStore != "MEMORY") {
            val sType = sessionStore.uppercase()
            if (sType.contains("POSTGRES")) return "POSTGRES"
            if (sType.contains("MONGO")) return "MONGODB"
            if (sType.contains("SQLITE")) return "SQLITE"
            if (sType.contains("MEMORY")) return "MEMORY"
        }

        val generalStore = configJson.getString("StoreType")
        if (generalStore != null) {
            val gType = generalStore.uppercase()
            if (gType.contains("POSTGRES")) return "POSTGRES"
            if (gType.contains("MONGO")) return "MONGODB"
            if (gType.contains("SQLITE")) return "SQLITE"
            if (gType.contains("MEMORY")) return "MEMORY"
        }

        return "SQLITE"
    }
}
