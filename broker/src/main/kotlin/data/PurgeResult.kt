package at.rocworks.data

data class PurgeResult(
    val deletedCount: Int,
    val elapsedTimeMs: Long
)