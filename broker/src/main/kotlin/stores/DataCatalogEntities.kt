package at.rocworks.stores

import io.vertx.core.json.JsonObject
import java.time.Instant

data class DataCatalogType(
    val id: String,
    val namespace: String,
    val name: String,
    val description: String?,
    val structure: JsonObject,
    val topicPattern: String?,
    val createdAt: Instant? = null,
    val updatedAt: Instant? = null
)

data class DataCatalogInstance(
    val id: String,
    val typeId: String,
    val name: String,
    val baseTopic: String,
    val properties: JsonObject,
    val createdAt: Instant? = null,
    val updatedAt: Instant? = null
)

data class DataCatalogRelation(
    val sourceId: String,
    val targetId: String,
    val relationType: String
)

data class ImportDataCatalogResult(
    val success: Boolean,
    val typesImported: Int = 0,
    val instancesImported: Int = 0,
    val relationsImported: Int = 0,
    val failed: Int = 0,
    val errors: List<String> = emptyList()
) {
    companion object {
        fun success(types: Int, instances: Int, relations: Int): ImportDataCatalogResult =
            ImportDataCatalogResult(true, types, instances, relations, 0)
        fun failure(errors: List<String>): ImportDataCatalogResult =
            ImportDataCatalogResult(false, 0, 0, 0, errors.size, errors)
    }
}
