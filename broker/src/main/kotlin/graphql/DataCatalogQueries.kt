package at.rocworks.graphql

import at.rocworks.stores.IDataCatalogStore
import at.rocworks.stores.DataCatalogType
import at.rocworks.stores.DataCatalogInstance
import at.rocworks.stores.DataCatalogRelation
import graphql.schema.DataFetcher
import java.util.concurrent.CompletableFuture

class DataCatalogQueries(
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
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
            if (dataCatalogStore == null) {
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
}
