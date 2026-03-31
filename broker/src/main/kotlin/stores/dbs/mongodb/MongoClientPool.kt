package at.rocworks.stores.mongodb

import at.rocworks.Utils
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Shared MongoClient pool that caches clients by connection string.
 *
 * All MongoDB stores using the same connection string share a single MongoClient
 * (and its connection pool), preventing connection multiplication and reconnection storms.
 *
 * Uses reference counting: the underlying MongoClient is closed only when all stores
 * have released their reference.
 */
object MongoClientPool {

    private val logger = Utils.getLogger(MongoClientPool::class.java)

    private class PoolEntry(
        val client: MongoClient,
        val refCount: AtomicInteger = AtomicInteger(1)
    )

    private val pool = ConcurrentHashMap<String, PoolEntry>()
    private val lock = ReentrantLock()

    /** Default socket read timeout used when creating new clients. */
    @Volatile
    var defaultReadTimeoutMs: Long = 60000

    /**
     * Get or create a shared MongoClient for the given connection string.
     * Each call increments the reference count; callers must call [releaseClient] when done.
     */
    fun getClient(connectionString: String): MongoClient {
        lock.withLock {
            val existing = pool[connectionString]
            if (existing != null) {
                existing.refCount.incrementAndGet()
                logger.fine { "Reusing shared MongoClient for [$connectionString] (refs=${existing.refCount.get()})" }
                return existing.client
            }

            val settings = MongoClientSettingsFactory.createSettings(connectionString, readTimeoutMs = defaultReadTimeoutMs)
            val client = MongoClients.create(settings)
            pool[connectionString] = PoolEntry(client)
            logger.info("Created shared MongoClient for [$connectionString] (refs=1, readTimeoutMs=$defaultReadTimeoutMs)")
            return client
        }
    }

    /**
     * Get or create a shared MongoClient with custom settings overrides.
     * Use this when a store needs specific settings (e.g. custom WriteConcern).
     */
    fun getClient(connectionString: String, settings: com.mongodb.MongoClientSettings): MongoClient {
        lock.withLock {
            val existing = pool[connectionString]
            if (existing != null) {
                existing.refCount.incrementAndGet()
                logger.fine { "Reusing shared MongoClient for [$connectionString] (refs=${existing.refCount.get()})" }
                return existing.client
            }

            val client = MongoClients.create(settings)
            pool[connectionString] = PoolEntry(client)
            logger.info("Created shared MongoClient for [$connectionString] (refs=1)")
            return client
        }
    }

    /**
     * Release a reference to the shared MongoClient. When the last reference is released,
     * the client is closed and removed from the pool.
     */
    fun releaseClient(connectionString: String) {
        lock.withLock {
            val entry = pool[connectionString] ?: return
            val remaining = entry.refCount.decrementAndGet()
            logger.fine { "Released MongoClient for [$connectionString] (refs=$remaining)" }
            if (remaining <= 0) {
                pool.remove(connectionString)
                try {
                    entry.client.close()
                    logger.info("Closed shared MongoClient for [$connectionString] (no more references)")
                } catch (e: Exception) {
                    logger.warning("Error closing shared MongoClient: ${e.message}")
                }
            }
        }
    }
}
