package at.rocworks

import java.io.Serializable

class TopicName(val identifier: String): Serializable {
    override fun toString() = identifier

    fun isWildCardTopic(): Boolean = identifier.contains('+') || identifier.contains('#')

    fun getLevels() = identifier.split("/")

    fun addLevel(level: String) = TopicName("$identifier/$level")

    fun matchesToWildcard(wildcardTopicName: TopicName): Boolean {
        val wildcardLevels = wildcardTopicName.getLevels()
        val topicLevels = getLevels()

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