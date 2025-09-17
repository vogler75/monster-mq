package at.rocworks.stores

import at.rocworks.handlers.ArchiveGroup
import io.vertx.core.Future

data class ArchiveGroupConfig(
    val archiveGroup: ArchiveGroup,
    val enabled: Boolean
)

interface IConfigStore {
    fun getType(): String

    fun getAllArchiveGroups(): Future<List<ArchiveGroupConfig>>
    fun getArchiveGroup(name: String): Future<ArchiveGroupConfig?>
    fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean>
    fun deleteArchiveGroup(name: String): Future<Boolean>
    fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Future<Boolean>
}