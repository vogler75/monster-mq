package at.rocworks.stores

import io.vertx.core.Future
import io.vertx.core.json.JsonObject

interface IDataCatalogStore {
    fun initialize(): Future<Void>

    // Types
    fun getTypes(namespace: String? = null): Future<List<DataCatalogType>>
    fun getType(id: String): Future<DataCatalogType?>
    fun saveType(type: DataCatalogType): Future<DataCatalogType>
    fun deleteType(id: String): Future<Boolean>

    // Instances
    fun getInstances(typeId: String? = null): Future<List<DataCatalogInstance>>
    fun getInstance(id: String): Future<DataCatalogInstance?>
    fun saveInstance(instance: DataCatalogInstance): Future<DataCatalogInstance>
    fun deleteInstance(id: String): Future<Boolean>

    // Relations
    fun getRelations(sourceId: String? = null, targetId: String? = null, relationType: String? = null): Future<List<DataCatalogRelation>>
    fun saveRelation(relation: DataCatalogRelation): Future<DataCatalogRelation>
    fun deleteRelation(sourceId: String, targetId: String, relationType: String): Future<Boolean>

    fun close(): Future<Void>

    // Import/Export
    fun exportCatalog(namespace: String? = null): Future<JsonObject>
    fun importCatalog(data: JsonObject): Future<ImportDataCatalogResult>
}
