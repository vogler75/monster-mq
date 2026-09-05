package at.rocworks.graphql

import at.rocworks.Features
import at.rocworks.Monster
import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import graphql.schema.DataFetcher
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture

class DataCatalogMutations(
    private val dataCatalogStore: IDataCatalogStore?
) {

    private fun requireText(value: String?, field: String): String =
        value?.trim()?.takeIf { it.isNotEmpty() }
            ?: throw IllegalArgumentException("$field must be a non-empty string")

    private fun entityExists(id: String): Future<Boolean> {
        val store = dataCatalogStore ?: return Future.succeededFuture(false)
        return store.getType(id).compose { type ->
            if (type != null) Future.succeededFuture(true)
            else store.getInstance(id).map { it != null }
        }
    }

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

    @Suppress("UNCHECKED_CAST")
    private fun toJsonObject(raw: Any?): JsonObject {
        return when (raw) {
            is Map<*, *> -> JsonObject(raw as Map<String, Any>)
            is JsonObject -> raw
            else -> JsonObject()
        }
    }

    private fun relationToMap(relation: DataCatalogRelation): Map<String, Any?> {
        return mapOf(
            "sourceId" to relation.sourceId,
            "targetId" to relation.targetId,
            "relationType" to relation.relationType
        )
    }

    fun saveType(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val input = env.getArgument<Map<String, Any>>("input") ?: emptyMap()
            val type = try { DataCatalogType(
                id = requireText(input["id"] as? String, "id"),
                namespace = requireText(input["namespace"] as? String, "namespace"),
                name = requireText(input["name"] as? String, "name"),
                description = input["description"] as? String,
                structure = toJsonObject(input["structure"]),
                topicPattern = input["topicPattern"] as? String
            ) } catch (e: Exception) {
                future.completeExceptionally(e)
                return@DataFetcher future
            }
            dataCatalogStore.saveType(type).onComplete { res ->
                if (res.succeeded()) {
                    future.complete(typeToMap(res.result()))
                } else {
                    future.completeExceptionally(res.cause())
                }
            }
            future
        }
    }

    fun deleteType(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val id = env.getArgument<String>("id") ?: ""
            dataCatalogStore.deleteType(id).onComplete { res ->
                if (res.succeeded()) future.complete(res.result())
                else future.completeExceptionally(res.cause())
            }
            future
        }
    }

    fun saveInstance(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val input = env.getArgument<Map<String, Any>>("input") ?: emptyMap()
            val instance = try { DataCatalogInstance(
                id = requireText(input["id"] as? String, "id"),
                typeId = requireText(input["typeId"] as? String, "typeId"),
                name = requireText(input["name"] as? String, "name"),
                baseTopic = requireText(input["baseTopic"] as? String, "baseTopic").also {
                    require(!it.contains('+') && !it.contains('#') && !it.contains('\u0000')) {
                        "baseTopic must be a concrete MQTT topic"
                    }
                },
                properties = toJsonObject(input["properties"])
            ) } catch (e: Exception) {
                future.completeExceptionally(e)
                return@DataFetcher future
            }
            dataCatalogStore.getType(instance.typeId).compose { type ->
                if (type == null) Future.failedFuture("Unknown catalog type: ${instance.typeId}")
                else dataCatalogStore.saveInstance(instance)
            }.onComplete { res ->
                if (res.succeeded()) {
                    future.complete(instanceToMap(res.result()))
                } else {
                    future.completeExceptionally(res.cause())
                }
            }
            future
        }
    }

    fun deleteInstance(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val id = env.getArgument<String>("id") ?: ""
            dataCatalogStore.deleteInstance(id).onComplete { res ->
                if (res.succeeded()) future.complete(res.result())
                else future.completeExceptionally(res.cause())
            }
            future
        }
    }

    fun saveRelation(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val input = env.getArgument<Map<String, Any>>("input") ?: emptyMap()
            val relation = try { DataCatalogRelation(
                sourceId = requireText(input["sourceId"] as? String, "sourceId"),
                targetId = requireText(input["targetId"] as? String, "targetId"),
                relationType = requireText(input["relationType"] as? String, "relationType")
            ).also { require(it.sourceId != it.targetId) { "sourceId and targetId must be different" } } }
            catch (e: Exception) {
                future.completeExceptionally(e)
                return@DataFetcher future
            }
            entityExists(relation.sourceId).compose { sourceExists ->
                if (!sourceExists) Future.failedFuture("Unknown relation source: ${relation.sourceId}")
                else entityExists(relation.targetId)
            }.compose { targetExists ->
                if (!targetExists) Future.failedFuture("Unknown relation target: ${relation.targetId}")
                else dataCatalogStore.saveRelation(relation)
            }.onComplete { res ->
                if (res.succeeded()) {
                    future.complete(relationToMap(res.result()))
                } else {
                    future.completeExceptionally(res.cause())
                }
            }
            future
        }
    }

    fun deleteRelation(): DataFetcher<CompletableFuture<Boolean>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Boolean>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val sourceId = env.getArgument<String>("sourceId") ?: ""
            val targetId = env.getArgument<String>("targetId") ?: ""
            val relationType = env.getArgument<String>("relationType") ?: ""
            dataCatalogStore.deleteRelation(sourceId, targetId, relationType).onComplete { res ->
                if (res.succeeded()) future.complete(res.result())
                else future.completeExceptionally(res.cause())
            }
            future
        }
    }

    fun exportCatalog(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val namespace = env.getArgument<String>("namespace")
            dataCatalogStore.exportCatalog(namespace).onComplete { res ->
                if (res.succeeded()) {
                    future.complete(res.result().map)
                } else {
                    future.completeExceptionally(res.cause())
                }
            }
            future
        }
    }

    fun importCatalog(): DataFetcher<CompletableFuture<Map<String, Any?>>> {
        return DataFetcher { env ->
            val future = CompletableFuture<Map<String, Any?>>()
            if (!Monster.isFeatureEnabled(Features.DataCatalog) || dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val data = env.getArgument<Map<String, Any>>("data") ?: emptyMap()
            dataCatalogStore.importCatalog(JsonObject(data)).onComplete { res ->
                if (res.succeeded()) {
                    val result = res.result()
                    future.complete(mapOf(
                        "success" to result.success,
                        "typesImported" to result.typesImported,
                        "instancesImported" to result.instancesImported,
                        "relationsImported" to result.relationsImported,
                        "failed" to result.failed,
                        "errors" to result.errors
                    ))
                } else {
                    future.completeExceptionally(res.cause())
                }
            }
            future
        }
    }
}
