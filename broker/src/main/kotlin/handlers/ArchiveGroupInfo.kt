package at.rocworks.handlers

data class ArchiveGroupInfo(
    val archiveGroup: ArchiveGroup,
    val deploymentId: String,
    val enabled: Boolean
)
