package at.rocworks.stores

import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.Future

enum class DatabaseConnectionType {
    POSTGRES,
    MONGODB
}

data class DatabaseConnectionConfig(
    val name: String,
    val type: DatabaseConnectionType,
    val url: String,
    val username: String? = null,
    val password: String? = null,
    val database: String? = null,
    val schema: String? = null,
    val readOnly: Boolean = false,
    val createdAt: String? = null,
    val updatedAt: String? = null
)

data class ArchiveGroupConfig(
    val archiveGroup: ArchiveGroup,
    val enabled: Boolean
)

interface IArchiveConfigStore {
    fun getType(): String

    fun getAllArchiveGroups(): Future<List<ArchiveGroupConfig>>
    fun getArchiveGroup(name: String): Future<ArchiveGroupConfig?>
    fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean>
    fun deleteArchiveGroup(name: String): Future<Boolean>
    fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean>

    fun getAllDatabaseConnections(): Future<List<DatabaseConnectionConfig>> =
        Future.succeededFuture(emptyList())

    fun getDatabaseConnection(name: String): Future<DatabaseConnectionConfig?> =
        Future.succeededFuture(null)

    fun saveDatabaseConnection(connection: DatabaseConnectionConfig): Future<Boolean> =
        Future.succeededFuture(false)

    fun deleteDatabaseConnection(name: String): Future<Boolean> =
        Future.succeededFuture(false)

    /**
     * Check if the underlying storage table/collection exists.
     * Used in cluster scenarios to verify table creation by leader.
     */
    suspend fun tableExists(): Boolean
}
