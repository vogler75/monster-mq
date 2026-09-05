package at.rocworks.stores

import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/** Portable catalog documents contain only relations whose endpoints are included. */
object DataCatalogTransfer {
    fun export(store: IDataCatalogStore, namespace: String?): Future<JsonObject> =
        store.getTypes(namespace).compose { types ->
            store.getInstances().compose { allInstances ->
                val typeIds = types.map { it.id }.toSet()
                val instances = allInstances.filter { it.typeId in typeIds }
                val ids = typeIds + instances.map { it.id }
                store.getRelations().map { relations ->
                    JsonObject()
                        .put("types", JsonArray(types.map { t -> JsonObject()
                            .put("id", t.id).put("namespace", t.namespace).put("name", t.name)
                            .put("description", t.description).put("structure", t.structure)
                            .put("topicPattern", t.topicPattern) }))
                        .put("instances", JsonArray(instances.map { i -> JsonObject()
                            .put("id", i.id).put("typeId", i.typeId).put("name", i.name)
                            .put("baseTopic", i.baseTopic).put("properties", i.properties) }))
                        .put("relations", JsonArray(relations.filter { it.sourceId in ids && it.targetId in ids }
                            .distinct().map { r -> JsonObject().put("sourceId", r.sourceId)
                                .put("targetId", r.targetId).put("relationType", r.relationType) }))
                }
            }
        }

    /** Validate the entire document before writing; report actual progress on a storage failure. */
    fun import(store: IDataCatalogStore, data: JsonObject): Future<ImportDataCatalogResult> {
        val types: List<DataCatalogType>
        val instances: List<DataCatalogInstance>
        val relations: List<DataCatalogRelation>
        try {
            require(listOf("types", "instances", "relations").any { data.containsKey(it) }) {
                "Expected a catalog document containing types, instances or relations"
            }
            types = objects(data, "types").map { t -> DataCatalogType(
                required(t, "id"), required(t, "namespace"), required(t, "name"),
                t.getString("description"), t.getJsonObject("structure")
                    ?: throw IllegalArgumentException("structure must be a JSON object"), t.getString("topicPattern")) }
            instances = objects(data, "instances").map { i -> DataCatalogInstance(
                required(i, "id"), required(i, "typeId"), required(i, "name"), required(i, "baseTopic"),
                i.getJsonObject("properties") ?: throw IllegalArgumentException("properties must be a JSON object")) }
            relations = objects(data, "relations").map { r -> DataCatalogRelation(
                required(r, "sourceId"), required(r, "targetId"), required(r, "relationType")) }
            require(types.map { it.id }.distinct().size == types.size) { "Duplicate type IDs" }
            require(instances.map { it.id }.distinct().size == instances.size) { "Duplicate instance IDs" }
            require(relations.distinct().size == relations.size) { "Duplicate relations" }
            relations.forEach { require(it.sourceId != it.targetId) { "Self relationships are not allowed: ${it.sourceId}" } }
            instances.forEach { require(!it.baseTopic.contains('+') && !it.baseTopic.contains('#') && !it.baseTopic.contains('\u0000')) {
                "baseTopic must be a concrete MQTT topic: ${it.id}"
            } }
        } catch (e: Exception) {
            return Future.succeededFuture(ImportDataCatalogResult.failure(listOf(e.message ?: "Invalid catalog")))
        }
        return store.getTypes().compose { existingTypes ->
            store.getInstances().compose { existingInstances ->
                val typeIds = (existingTypes + types).map { it.id }.toSet()
                val instanceIds = (existingInstances + instances).map { it.id }.toSet()
                val ids = typeIds + instanceIds
                val errors = mutableListOf<String>()
                instances.filter { it.typeId !in typeIds }.forEach { errors.add("Unknown type ${it.typeId} for ${it.id}") }
                relations.filter { it.sourceId !in ids || it.targetId !in ids }.forEach {
                    errors.add("Unknown relation endpoint: ${it.sourceId} -> ${it.targetId}")
                }
                if (errors.isNotEmpty()) Future.succeededFuture(ImportDataCatalogResult.failure(errors))
                else write(store, types, instances, relations)
            }
        }.recover { Future.succeededFuture(ImportDataCatalogResult.failure(listOf(it.message ?: "Import failed"))) }
    }

    private fun write(store: IDataCatalogStore, types: List<DataCatalogType>, instances: List<DataCatalogInstance>,
                      relations: List<DataCatalogRelation>): Future<ImportDataCatalogResult> {
        var typesCount = 0
        var instancesCount = 0
        var relationsCount = 0
        var writes: Future<Void> = Future.succeededFuture()
        types.forEach { t -> writes = writes.compose { store.saveType(t).map<Void> { typesCount++; null } } }
        instances.forEach { i -> writes = writes.compose { store.saveInstance(i).map<Void> { instancesCount++; null } } }
        relations.forEach { r -> writes = writes.compose { store.saveRelation(r).map<Void> { relationsCount++; null } } }
        return writes.map { ImportDataCatalogResult.success(typesCount, instancesCount, relationsCount) }
            .recover { error -> Future.succeededFuture(ImportDataCatalogResult(
                false, typesCount, instancesCount, relationsCount, 1,
                listOf("Import stopped after $typesCount types, $instancesCount instances and $relationsCount relations: ${error.message}"))) }
    }

    private fun objects(data: JsonObject, key: String): List<JsonObject> {
        if (!data.containsKey(key)) return emptyList()
        val array = data.getJsonArray(key) ?: throw IllegalArgumentException("$key must be an array")
        return (0 until array.size()).map { array.getJsonObject(it)
            ?: throw IllegalArgumentException("$key[$it] must be an object") }
    }

    private fun required(obj: JsonObject, key: String): String =
        obj.getString(key)?.takeIf { it.isNotBlank() }
            ?: throw IllegalArgumentException("$key must be a non-empty string")
}
