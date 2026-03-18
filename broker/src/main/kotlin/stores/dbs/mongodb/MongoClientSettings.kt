package at.rocworks.stores.mongodb

import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import java.util.concurrent.TimeUnit

/**
 * Shared MongoDB client settings with sensible timeouts to prevent Vert.x event loop blocking.
 *
 * Default MongoDB serverSelectionTimeout is 30s, which causes thread starvation when MongoDB
 * is slow or unavailable. This helper sets it to 5s along with pool and socket timeouts.
 */
object MongoClientSettingsFactory {

    fun createSettings(
        connectionString: String,
        poolMaxSize: Int = 50,
        poolMinSize: Int = 10,
        poolMaxWaitTimeMs: Long = 2000,
        poolMaxLifeTimeMinutes: Long = 30,
        poolMaxIdleTimeMinutes: Long = 10,
        connectTimeoutMs: Long = 5000,
        readTimeoutMs: Long = 10000,
        serverSelectionTimeoutMs: Long = 5000
    ): MongoClientSettings {
        return MongoClientSettings.builder()
            .applyConnectionString(ConnectionString(connectionString))
            .applyToConnectionPoolSettings { builder ->
                builder.maxSize(poolMaxSize)
                builder.minSize(poolMinSize)
                builder.maxWaitTime(poolMaxWaitTimeMs, TimeUnit.MILLISECONDS)
                builder.maxConnectionLifeTime(poolMaxLifeTimeMinutes, TimeUnit.MINUTES)
                builder.maxConnectionIdleTime(poolMaxIdleTimeMinutes, TimeUnit.MINUTES)
            }
            .applyToSocketSettings { builder ->
                builder.connectTimeout(connectTimeoutMs, TimeUnit.MILLISECONDS)
                builder.readTimeout(readTimeoutMs, TimeUnit.MILLISECONDS)
            }
            .applyToClusterSettings { builder ->
                builder.serverSelectionTimeout(serverSelectionTimeoutMs, TimeUnit.MILLISECONDS)
            }
            .build()
    }
}
