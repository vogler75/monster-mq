package at.rocworks

object TopicUtils {
    fun isWildCardTopic(topicName: String): Boolean = topicName.contains('+') || topicName.contains('#')

    fun topicMatches(wildcardTopic: String, topic: String): Boolean {
        val wildcardLevels = wildcardTopic.split("/")
        val topicLevels = topic.split("/")

        // Index to iterate through both lists
        var i = 0

        while (i < wildcardLevels.size) {
            val wildcardLevel = wildcardLevels[i]

            if (wildcardLevel == "#") {
                // The rest of the topic matches due to the multi-level wildcard
                return true
            } else if (wildcardLevel == "+") {
                // Single-level wildcard, skip this level
                if (i >= topicLevels.size) {
                    // If we run out of topic levels
                    return false
                }
            } else {
                // Exact match required
                if (i >= topicLevels.size || wildcardLevel != topicLevels[i]) {
                    return false
                }
            }

            i++
        }

        // If we've checked all levels in the wildcard topic and the topic has no more levels left
        return i == topicLevels.size
    }
}