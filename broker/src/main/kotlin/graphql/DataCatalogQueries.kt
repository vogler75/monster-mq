package at.rocworks.graphql

import at.rocworks.Features
import at.rocworks.Monster
import at.rocworks.handlers.ArchiveHandler
import at.rocworks.data.BrokerMessage
import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import graphql.schema.DataFetcher
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture

class DataCatalogQueries(
    private val dataCatalogStore: IDataCatalogStore?,
    private val archiveHandler: ArchiveHandler? = null
) {

    private fun typeToMap(type: DataCatalogType): Map<String, Any?> {
        return mapOf(
            "id" to type.id,
            "namespace" to type.namespace,
            "name" to type.name,
            "description" to type.description,
            "structure" to type.structure.map,
            "topicPattern" to type.topicPattern,
            "createdAt" to type.createdAt?.toString(),
            "updatedAt" to type.updatedAt?.toString()
        )
    }

    private fun instanceToMap(instance: DataCatalogInstance): Map<String, Any?> {
        return mapOf(
            "id" to instance.id,
            "typeId" to instance.typeId,
            "name" to instance.name,
            "baseTopic" to instance.baseTopic,
            "properties" to instance.properties.map,
            "createdAt" to instance.createdAt?.toString(),
            "updatedAt" to instance.updatedAt?.toString()
        )
    }

    private fun relationToMap(relation: DataCatalogRelation): Map<String, Any?> {
        return mapOf(
            "sourceId" to relation.sourceId,
            "targetId" to relation.targetId,
            "relationType" to relation.relationType
        )
    }

    fun dataCatalogTypes(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            val namespace = env.getArgument<String>("namespace")
            dataCatalogStore.getTypes(namespace).onComplete { res ->
                if (res.succeeded()) {
                    future.complete(res.result().map { typeToMap(it) })
                } else {
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun dataCatalogType(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.complete(null)
                return@DataFetcher future
            }
            val id = env.getArgument<String>("id")
            if (id == null) {
                future.complete(null)
                return@DataFetcher future
            }
            dataCatalogStore.getType(id).onComplete { res ->
                if (res.succeeded() && res.result() != null) {
                    future.complete(typeToMap(res.result()!!))
                } else {
                    future.complete(null)
                }
            }
            future
        }
    }

    fun dataCatalogInstances(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            val typeId = env.getArgument<String>("typeId")
            dataCatalogStore.getInstances(typeId).onComplete { res ->
                if (res.succeeded()) {
                    future.complete(res.result().map { instanceToMap(it) })
                } else {
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun dataCatalogInstance(): DataFetcher<CompletableFuture<Map<String, Any?>?>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>?>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.complete(null)
                return@DataFetcher future
            }
            val id = env.getArgument<String>("id")
            if (id == null) {
                future.complete(null)
                return@DataFetcher future
            }
            dataCatalogStore.getInstance(id).onComplete { res ->
                if (res.succeeded() && res.result() != null) {
                    future.complete(instanceToMap(res.result()!!))
                } else {
                    future.complete(null)
                }
            }
            future
        }
    }

    fun dataCatalogRelations(): DataFetcher<CompletableFuture<List<Map<String, Any?>>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<List<Map<String, Any?>>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.complete(emptyList())
                return@DataFetcher future
            }
            val sourceId = env.getArgument<String>("sourceId")
            val targetId = env.getArgument<String>("targetId")
            val relationType = env.getArgument<String>("relationType")
            
            dataCatalogStore.getRelations(sourceId, targetId, relationType).onComplete { res ->
                if (res.succeeded()) {
                    future.complete(res.result().map { relationToMap(it) })
                } else {
                    future.complete(emptyList())
                }
            }
            future
        }
    }

    fun inferDataCatalog(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            val archiveGroupName = env.getArgument<String>("archiveGroup") ?: "Default"
            val topicPattern = env.getArgument<String>("topicPattern") ?: "#"

            if (!Monster.isFeatureEnabled(Features.DataCatalog) || archiveHandler == null) {
                future.complete(mapOf(
                    "types" to emptyList<Map<String, Any?>>(),
                    "instances" to emptyList<Map<String, Any?>>(),
                    "relations" to emptyList<Map<String, Any?>>(),
                    "topicsAnalyzed" to 0,
                    "summary" to null,
                    "error" to "ArchiveHandler is not available"
                ))
                return@DataFetcher future
            }

            val archiveGroup = archiveHandler.getDeployedArchiveGroups()[archiveGroupName]
            if (archiveGroup == null || archiveGroup.lastValStore == null) {
                future.complete(mapOf(
                    "types" to emptyList<Map<String, Any?>>(),
                    "instances" to emptyList<Map<String, Any?>>(),
                    "relations" to emptyList<Map<String, Any?>>(),
                    "topicsAnalyzed" to 0,
                    "summary" to null,
                    "error" to "Archive group '$archiveGroupName' or LastValueStore not found"
                ))
                return@DataFetcher future
            }

            val lastValStore = archiveGroup.lastValStore!!
            val messages = mutableListOf<BrokerMessage>()
            lastValStore.findMatchingMessages(topicPattern) { msg ->
                if (messages.size < 300) {
                    messages.add(msg)
                    true
                } else {
                    false
                }
            }

            if (messages.isEmpty()) {
                future.complete(mapOf(
                    "types" to emptyList<Map<String, Any?>>(),
                    "instances" to emptyList<Map<String, Any?>>(),
                    "relations" to emptyList<Map<String, Any?>>(),
                    "topicsAnalyzed" to 0,
                    "summary" to "No topics found matching pattern '$topicPattern'",
                    "error" to null
                ))
                return@DataFetcher future
            }

            val proposedTypes = mutableMapOf<String, DataCatalogType>()
            val proposedInstances = mutableMapOf<String, DataCatalogInstance>()
            val proposedRelations = mutableSetOf<DataCatalogRelation>()

            // 1. Inspect topics and payloads
            val jsonTopics = mutableListOf<Pair<String, JsonObject>>()
            val leafTopicsByParent = mutableMapOf<String, MutableMap<String, String>>()

            for (msg in messages) {
                val topic = msg.topicName
                val payloadStr = try { msg.payload.toString(Charsets.UTF_8).trim() } catch (e: Exception) { null }

                if (payloadStr != null && payloadStr.startsWith("{") && payloadStr.endsWith("}")) {
                    try {
                        val json = JsonObject(payloadStr)
                        jsonTopics.add(topic to json)
                        continue
                    } catch (e: Exception) { /* fallback to leaf */ }
                }

                // Treat as leaf topic
                val segments = topic.split('/')
                if (segments.size > 1) {
                    val parent = segments.dropLast(1).joinToString("/")
                    val leaf = segments.last()
                    val leafType = when {
                        payloadStr == null -> "string"
                        payloadStr.toDoubleOrNull() != null -> "number"
                        payloadStr.equals("true", true) || payloadStr.equals("false", true) -> "boolean"
                        else -> "string"
                    }
                    leafTopicsByParent.computeIfAbsent(parent) { mutableMapOf() }[leaf] = leafType
                } else {
                    // Single level topic
                    val leafType = when {
                        payloadStr == null -> "string"
                        payloadStr.toDoubleOrNull() != null -> "number"
                        payloadStr.equals("true", true) || payloadStr.equals("false", true) -> "boolean"
                        else -> "string"
                    }
                    leafTopicsByParent.computeIfAbsent(topic) { mutableMapOf() }["value"] = leafType
                }
            }

            // 2. Process JSON payload topics
            for ((topic, json) in jsonTopics) {
                val segments = topic.split('/')
                val entityName = segments.lastOrNull() ?: topic
                val typeName = segments.dropLast(1).lastOrNull()?.replaceFirstChar { it.uppercase() } ?: "Device"
                val typeId = typeName.lowercase() + "-type"

                // Infer schema properties
                val propertiesObj = JsonObject()
                for (key in json.fieldNames()) {
                    val value = json.getValue(key)
                    val propSchema = JsonObject()
                    when (value) {
                        is Number -> propSchema.put("type", "number")
                        is Boolean -> propSchema.put("type", "boolean")
                        is JsonObject -> propSchema.put("type", "object")
                        is JsonArray -> propSchema.put("type", "array")
                        else -> propSchema.put("type", "string")
                    }
                    propertiesObj.put(key, propSchema)
                }

                val schema = JsonObject()
                    .put("\$schema", "http://json-schema.org/draft-07/schema#")
                    .put("type", "object")
                    .put("title", typeName)
                    .put("properties", propertiesObj)

                val wildcardPattern = if (segments.size > 1) {
                    segments.dropLast(1).joinToString("/") + "/+"
                } else {
                    topic
                }

                if (!proposedTypes.containsKey(typeId)) {
                    proposedTypes[typeId] = DataCatalogType(
                        id = typeId,
                        namespace = "default",
                        name = typeName,
                        description = "Inferred type for $typeName entities",
                        structure = schema,
                        topicPattern = wildcardPattern
                    )
                }

                val instanceId = topic.replace('/', '-')
                proposedInstances[instanceId] = DataCatalogInstance(
                    id = instanceId,
                    typeId = typeId,
                    name = entityName,
                    baseTopic = topic,
                    properties = JsonObject().put("inferred", true)
                )

                // Build hierarchy relations
                buildHierarchyRelations(segments, proposedInstances, proposedRelations)
            }

            // 3. Process Leaf Telemetry Topics (grouped by parent baseTopic)
            for ((parentTopic, leaves) in leafTopicsByParent) {
                val segments = parentTopic.split('/')
                val entityName = segments.lastOrNull() ?: parentTopic
                val typeName = (segments.dropLast(1).lastOrNull() ?: "Telemetry").replaceFirstChar { it.uppercase() } + "Node"
                val typeId = typeName.lowercase() + "-type"

                val propertiesObj = JsonObject()
                for ((leafName, leafType) in leaves) {
                    propertiesObj.put(leafName, JsonObject().put("type", leafType))
                }

                val schema = JsonObject()
                    .put("\$schema", "http://json-schema.org/draft-07/schema#")
                    .put("type", "object")
                    .put("title", typeName)
                    .put("properties", propertiesObj)

                val wildcardPattern = if (segments.size > 1) {
                    segments.dropLast(1).joinToString("/") + "/+/#"
                } else {
                    "$parentTopic/#"
                }

                if (!proposedTypes.containsKey(typeId)) {
                    proposedTypes[typeId] = DataCatalogType(
                        id = typeId,
                        namespace = "default",
                        name = typeName,
                        description = "Inferred multi-topic telemetry type for $typeName",
                        structure = schema,
                        topicPattern = wildcardPattern
                    )
                }

                val instanceId = parentTopic.replace('/', '-')
                proposedInstances[instanceId] = DataCatalogInstance(
                    id = instanceId,
                    typeId = typeId,
                    name = entityName,
                    baseTopic = parentTopic,
                    properties = JsonObject().put("inferred", true)
                )

                buildHierarchyRelations(segments, proposedInstances, proposedRelations)
            }

            val resultTypes = proposedTypes.values.map { typeToMap(it) }
            val resultInstances = proposedInstances.values.map { instanceToMap(it) }
            val resultRelations = proposedRelations.map { relationToMap(it) }

            val summary = "Inferred ${resultTypes.size} object types, ${resultInstances.size} instances, and ${resultRelations.size} relations from ${messages.size} topics."

            future.complete(mapOf(
                "types" to resultTypes,
                "instances" to resultInstances,
                "relations" to resultRelations,
                "topicsAnalyzed" to messages.size,
                "summary" to summary,
                "error" to null
            ))
            future
        }
    }

    private fun buildHierarchyRelations(
        segments: List<String>,
        instances: MutableMap<String, DataCatalogInstance>,
        relations: MutableSet<DataCatalogRelation>
    ) {
        if (segments.size < 2) return
        for (i in 0 until segments.size - 1) {
            val sourcePath = segments.take(i + 1).joinToString("/")
            val targetPath = segments.take(i + 2).joinToString("/")
            val sourceId = sourcePath.replace('/', '-')
            val targetId = targetPath.replace('/', '-')

            relations.add(DataCatalogRelation(
                sourceId = sourceId,
                targetId = targetId,
                relationType = "HasComponent"
            ))
        }
    }
}
