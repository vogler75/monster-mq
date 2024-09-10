package at.rocworks

object Config {
    private var isCluster = false
    fun setClustered(clustered: Boolean) {
        isCluster = clustered
    }
    fun isClustered() = isCluster
}