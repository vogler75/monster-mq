package at.rocworks.stores

import at.rocworks.stores.ArchiveGroup
import io.vertx.core.Future

data class ArchiveGroupConfig(
    val archiveGroup: ArchiveGroup,
    val enabled: Boolean
)

interface IConfigStore {
    fun getType(): String

    fun getAllArchiveGroups(): List<ArchiveGroupConfig>
    fun getArchiveGroup(name: String): ArchiveGroupConfig?
    fun saveArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Boolean
    fun deleteArchiveGroup(name: String): Boolean
    fun updateArchiveGroup(archiveGroup: ArchiveGroup, enabled: Boolean): Boolean
}