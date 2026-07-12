package at.rocworks.bus

/** Converts validated MQTT topic filters and handles prefix translation to/from Zenoh key expressions. */
object ZenohTopicMapper {
    
    // Maps a local MQTT topic to a Zenoh key
    fun mapToZenohKey(topic: String, localPrefix: String, remotePrefix: String): String? {
        val trimmedLocal = localPrefix.trim('/')
        val trimmedRemote = remotePrefix.trim('/')
        val topicNormalized = topic.trim('/')

        val relativeTopic = if (trimmedLocal.isEmpty()) {
            topicNormalized
        } else {
            if (!topicNormalized.startsWith(trimmedLocal)) {
                return null
            }
            topicNormalized.removePrefix(trimmedLocal).trimStart('/')
        }

        if (relativeTopic.isEmpty()) return null
        return if (trimmedRemote.isEmpty()) relativeTopic else "$trimmedRemote/$relativeTopic"
    }

    // Maps a Zenoh key back to a local MQTT topic
    fun mapToMqttTopic(zenohKey: String, localPrefix: String, remotePrefix: String): String? {
        val trimmedLocal = localPrefix.trim('/')
        val trimmedRemote = remotePrefix.trim('/')
        val keyNormalized = zenohKey.trim('/')

        val relativeTopic = if (trimmedRemote.isEmpty()) {
            keyNormalized
        } else {
            if (!keyNormalized.startsWith(trimmedRemote)) {
                return null
            }
            keyNormalized.removePrefix(trimmedRemote).trimStart('/')
        }

        if (relativeTopic.isEmpty()) return null

        val mqttTopic = if (relativeTopic.contains('*')) {
            relativeTopic.split('/').joinToString("/") { level ->
                when (level) {
                    "*" -> "+"
                    "**" -> "#"
                    else -> level
                }
            }
        } else {
            relativeTopic
        }
        
        return if (trimmedLocal.isEmpty()) {
            mqttTopic
        } else {
            "$trimmedLocal/$mqttTopic"
        }
    }

    // Maps a local MQTT filter pattern (which may contain wildcards) to a Zenoh key expression
    fun subscriptionKey(localFilter: String, localPrefix: String, remotePrefix: String): String? {
        val trimmedLocal = localPrefix.trim('/')
        val trimmedRemote = remotePrefix.trim('/')
        val filterNormalized = localFilter.trim('/')

        val relativeFilter = if (trimmedLocal.isEmpty()) {
            filterNormalized
        } else {
            if (filterNormalized == "#" || filterNormalized == "+") {
                "#"
            } else {
                if (!filterNormalized.startsWith(trimmedLocal)) {
                    return null
                }
                filterNormalized.removePrefix(trimmedLocal).trimStart('/')
            }
        }

        if (relativeFilter.isEmpty()) return null
        return subscriptionKey(trimmedRemote, relativeFilter)
    }

    // Original helper to map relative filter to Zenoh key expression
    fun subscriptionKey(prefix: String, mqttFilter: String): String {
        require(mqttFilter.isNotEmpty()) { "MQTT topic filter must not be empty" }
        val levels = mqttFilter.split('/')
        levels.forEachIndexed { index, level ->
            require(level != "#" || index == levels.lastIndex) { "MQTT # wildcard must be the final level: $mqttFilter" }
            require(!level.contains('#') || level == "#") { "MQTT # wildcard must occupy a complete level: $mqttFilter" }
            require(!level.contains('+') || level == "+") { "MQTT + wildcard must occupy a complete level: $mqttFilter" }
        }
        val zenohFilter = levels.joinToString("/") {
            when (it) {
                "+" -> "*"
                "#" -> "**"
                else -> it
            }
        }
        val prefixTrimmed = prefix.trim('/')
        return if (prefixTrimmed.isEmpty()) zenohFilter else "$prefixTrimmed/$zenohFilter"
    }

    /** Remove filters whose complete language is already covered by another configured filter. */
    fun minimalFilters(filters: List<String>): List<String> = filters.distinct().filter { candidate ->
        filters.distinct().none { other -> other != candidate && includes(other, candidate) }
    }

    internal fun includes(covering: String, candidate: String): Boolean {
        val a = covering.split('/')
        val b = candidate.split('/')
        var index = 0
        while (index < a.size) {
            val coveringLevel = a[index]
            if (coveringLevel == "#") return true
            if (index >= b.size) return false
            val candidateLevel = b[index]
            if (candidateLevel == "#") return false
            if (coveringLevel != "+" && coveringLevel != candidateLevel) return false
            index++
        }
        return index == b.size
    }
}
