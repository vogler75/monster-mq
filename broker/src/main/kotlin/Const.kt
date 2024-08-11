package at.rocworks

object Const {
    private const val DIST_NAMESPACE = "D"
    private const val TOPIC_NAMESPACE = "T"
    private const val RETAINED_NAMESPACE = "R"
    private const val CLIENT_NAMESPACE = "C"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val BROKER_KEY = "Broker"

    fun getTopicBusAddr(deploymentID: String) = "$TOPIC_NAMESPACE/${deploymentID}"
    fun getRetainedTopicBusAddr() = RETAINED_NAMESPACE
    fun getDistBusAddr() = "$DIST_NAMESPACE" // /${deploymentID}"
    fun getClientBusAddr(clientId: ClientId) = "$CLIENT_NAMESPACE/${clientId.identifier}"
}