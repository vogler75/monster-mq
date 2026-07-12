package at.rocworks.bus

/** Converts validated MQTT topic filters to Zenoh key expressions. */
object ZenohTopicMapper {
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
        return "${prefix.trim('/')}/$zenohFilter"
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
