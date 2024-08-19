package at.rocworks

object Const {
    const val GLOBAL_DISTRIBUTOR_NAMESPACE = "D"
    const val GLOBAL_RETAINED_NAMESPACE = "R"

    private const val CLIENT_NAMESPACE = "C"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val BROKER_KEY = "Broker"

    fun getClientAddress(clientId: ClientId) = "${CLIENT_NAMESPACE}/${clientId.identifier}"
}