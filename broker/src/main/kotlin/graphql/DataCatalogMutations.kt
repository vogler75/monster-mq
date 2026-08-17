package at.rocworks.graphql

import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import graphql.schema.DataFetcher
import io.vertx.core.json.JsonObject
import java.util.concurrent.CompletableFuture

class DataCatalogMutations(
    private val dataCatalogStore: IDataCatalogStore?
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
            if (dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val input = env.getArgument<Map<String, Any>>("input") ?: emptyMap()
            val type = DataCatalogType(
                id = (input["id"] as? String) ?: "",
                namespace = (input["namespace"] as? String) ?: "",
                name = (input["name"] as? String) ?: "",
                description = input["description"] as? String,
                structure = toJsonObject(input["structure"]),
                topicPattern = input["topicPattern"] as? String
            )
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val input = env.getArgument<Map<String, Any>>("input") ?: emptyMap()
            val instance = DataCatalogInstance(
                id = (input["id"] as? String) ?: "",
                typeId = (input["typeId"] as? String) ?: "",
                name = (input["name"] as? String) ?: "",
                baseTopic = (input["baseTopic"] as? String) ?: "",
                properties = toJsonObject(input["properties"])
            )
            dataCatalogStore.saveInstance(instance).onComplete { res ->
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
                future.completeExceptionally(Exception("DataCatalogStore not initialized"))
                return@DataFetcher future
            }
            val input = env.getArgument<Map<String, Any>>("input") ?: emptyMap()
            val relation = DataCatalogRelation(
                sourceId = (input["sourceId"] as? String) ?: "",
                targetId = (input["targetId"] as? String) ?: "",
                relationType = (input["relationType"] as? String) ?: ""
            )
            dataCatalogStore.saveRelation(relation).onComplete { res ->
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
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
