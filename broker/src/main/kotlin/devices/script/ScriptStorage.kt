package at.rocworks.devices.script

import at.rocworks.Utils
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.core.json.JsonObject
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger

/**
 * Persistent key-value storage for a script that survives broker reloads and restarts.
 * Persisted into the database through IDeviceConfigStore matching MonsterMQ Edge schema.
 */
class ScriptStorage(
    private val scriptName: String,
    private val store: IDeviceConfigStore?,
    private val nodeId: String
) {
    companion object {
        const val STORAGE_NAMESPACE = "script-storage"
        const val STORAGE_TYPE = "ScriptStorage"
        const val STORAGE_PREFIX = "__script_kv_"
        private val logger: Logger = Utils.getLogger(ScriptStorage::class.java)
    }

    private val data = ConcurrentHashMap<String, Any>()

    fun load(): Future<Void> {
        if (store == null) return Future.succeededFuture()
        val promise = Promise.promise<Void>()
        val deviceKey = STORAGE_PREFIX + scriptName

        store.getDevice(deviceKey).onComplete { result ->
            if (result.succeeded()) {
                val dc = result.result()
                if (dc != null) {
                    try {
                        dc.config.map.forEach { (k, v) ->
                            if (v != null) data[k] = v
                        }
                    } catch (e: Exception) {
                        logger.warning("Error deserializing script storage for $scriptName: ${e.message}")
                    }
                }
                promise.complete()
            } else {
                logger.warning("Failed to load script storage for $scriptName: ${result.cause()?.message}")
                promise.complete() // Non-fatal, continue with empty map
            }
        }
        return promise.future()
    }

    @Suppress("unused")
    fun get(key: String, defaultValue: Any? = null): Any? {
        return data[key] ?: defaultValue
    }

    @Suppress("unused")
    fun set(key: String, value: Any?): Any? {
        if (value == null) {
            data.remove(key)
        } else {
            data[key] = value
        }
        persist()
        return value
    }

    @Suppress("unused")
    fun delete(key: String): Boolean {
        val removed = data.remove(key) != null
        if (removed) persist()
        return removed
    }

    @Suppress("unused")
    fun list(): Map<String, Any> {
        return HashMap(data)
    }

    private fun persist(): Future<Void> {
        if (store == null) return Future.succeededFuture()
        val promise = Promise.promise<Void>()

        val jsonConfig = JsonObject(HashMap(data))
        val device = DeviceConfig(
            name = STORAGE_PREFIX + scriptName,
            namespace = STORAGE_NAMESPACE,
            nodeId = nodeId,
            type = STORAGE_TYPE,
            enabled = true,
            config = jsonConfig,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )

        store.saveDevice(device).onComplete { res ->
            if (res.succeeded()) {
                promise.complete()
            } else {
                logger.warning("Failed to persist script storage for $scriptName: ${res.cause()?.message}")
                promise.fail(res.cause())
            }
        }
        return promise.future()
    }
}
