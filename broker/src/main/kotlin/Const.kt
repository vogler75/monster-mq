package at.rocworks

object Const {
    const val DIST_SUBSCRIBE_REQUEST = "DIST/SUBSCRIBE"

    private const val TOPIC_NAMESPACE = "TOPIC"

    fun getTopicBusAddr(topicName: String) = "$TOPIC_NAMESPACE/$topicName"
    fun isWildCardTopic(topicName: String): Boolean = topicName.contains('+') || topicName.contains('#')
}