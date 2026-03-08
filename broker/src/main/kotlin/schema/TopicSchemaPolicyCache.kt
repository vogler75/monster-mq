package at.rocworks.schema

import at.rocworks.Utils
import at.rocworks.bus.EventBusAddresses
import at.rocworks.data.TopicTree
import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.IDeviceConfigStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.logging.Logger

/**
 * Cluster-aware in-memory cache for topic schema enforcement.
 *
 * Loads [TopicNamespace] entries from the [IDeviceConfigStore], resolves
 * their referenced [TopicSchemaPolicy] schemas, compiles validators, and
 * stores them in a [TopicTree] for fast O(depth) topic matching — the same
 * data structure used for MQTT subscription delivery.
 *
 * Listens on [EventBusAddresses.SchemaPolicy.RELOAD] for cluster-wide
 * invalidation broadcasts and rebuilds the tree on each event.
 */
class TopicSchemaPolicyCache(
    private val vertx: Vertx,
    private val deviceStore: IDeviceConfigStore
) {
    companion object {
        @Volatile
        private var instance: TopicSchemaPolicyCache? = null

        fun getInstance(): TopicSchemaPolicyCache? = instance

        internal fun setInstance(cache: TopicSchemaPolicyCache) {
            instance = cache
        }
    }

    private val logger: Logger = Utils.getLogger(TopicSchemaPolicyCache::class.java)

    /** Immutable snapshot replaced atomically on reload. */
    private val namespaceTree = AtomicReference(TopicTree<String, CompiledNamespaceEntry>())

    val validatedCount = AtomicLong(0)
    val rejectedCount = AtomicLong(0)
    val parseErrorCount = AtomicLong(0)
    val schemaErrorCount = AtomicLong(0)

    fun start() {
        setInstance(this)
        logger.info("TopicSchemaPolicyCache started")
        vertx.eventBus().consumer<JsonObject>(EventBusAddresses.SchemaPolicy.RELOAD) {
            reload()
        }
        reload()
    }

    fun reload() {
        deviceStore.getAllDevices().onComplete { result ->
            if (result.succeeded()) {
                val allDevices = result.result()

                // Build a map of schema policy name -> compiled validator
                val policyValidators = mutableMapOf<String, JsonSchemaValidator>()
                allDevices
                    .filter { it.type == DeviceConfig.DEVICE_TYPE_TOPIC_SCHEMA_POLICY }
                    .forEach { device ->
                        try {
                            val schemaObj = device.config.getJsonObject("jsonSchema") ?: return@forEach
                            policyValidators[device.name] = JsonSchemaValidator(schemaObj)
                        } catch (e: Exception) {
                            logger.warning("Failed to compile schema policy '${device.name}': ${e.message}")
                        }
                    }

                // Build a new TopicTree with namespace entries
                val newTree = TopicTree<String, CompiledNamespaceEntry>()
                var count = 0
                allDevices
                    .filter { it.type == DeviceConfig.DEVICE_TYPE_TOPIC_NAMESPACE && it.enabled }
                    .forEach { device ->
                        try {
                            val config = device.config
                            val topicFilter = config.getString("topicFilter") ?: return@forEach
                            val schemaPolicyName = config.getString("schemaPolicyName") ?: return@forEach
                            val validator = policyValidators[schemaPolicyName]
                            if (validator == null) {
                                logger.warning("Namespace '${device.name}' references unknown schema policy '$schemaPolicyName'")
                                return@forEach
                            }
                            val entry = CompiledNamespaceEntry(
                                namespaceName = device.name,
                                topicFilter = topicFilter,
                                schemaPolicyName = schemaPolicyName,
                                validator = validator,
                                enforcementMode = config.getString("enforcementMode", "REJECT")
                            )
                            newTree.add(topicFilter, device.name, entry)
                            count++
                        } catch (e: Exception) {
                            logger.warning("Failed to load namespace '${device.name}': ${e.message}")
                        }
                    }

                // Atomic swap — publish path sees either old or new tree, never a partially built one
                namespaceTree.set(newTree)
                logger.info("Schema policy cache: $count namespace bindings, ${policyValidators.size} schema policies")
            } else {
                logger.warning("Failed to reload schema policies: ${result.cause()?.message}")
            }
        }
    }

    /**
     * Find the first matching namespace for a concrete MQTT topic.
     * Uses [TopicTree.findDataOfTopicName] for O(depth) matching — same
     * algorithm used by the broker for subscription delivery.
     */
    fun matchNamespace(topic: String): CompiledNamespaceEntry? {
        val matches = namespaceTree.get().findDataOfTopicName(topic)
        return if (matches.isNotEmpty()) matches[0].second else null
    }

    fun publishReloadEvent() {
        vertx.eventBus().publish(EventBusAddresses.SchemaPolicy.RELOAD, JsonObject())
    }
}

/**
 * A compiled namespace entry ready for runtime validation.
 * Binds a topic filter to a compiled JSON Schema validator.
 */
data class CompiledNamespaceEntry(
    val namespaceName: String,
    val topicFilter: String,
    val schemaPolicyName: String,
    val validator: JsonSchemaValidator,
    val enforcementMode: String
)
