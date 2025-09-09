package at.rocworks.stores

data class PurgeResult(
    val deletedCount: Int,
    val elapsedTimeMs: Long
)