package at.rocworks.utils

import java.time.Duration

object DurationParser {
    fun parse(duration: String?): Long? {
        if (duration == null || duration.isEmpty()) return null
        
        val regex = """(\d+)([smhdwMy])""".toRegex()
        val match = regex.matchEntire(duration.trim()) ?: throw IllegalArgumentException("Invalid duration format: $duration")
        
        val value = match.groupValues[1].toLong()
        val unit = match.groupValues[2]
        
        return when (unit) {
            "s" -> Duration.ofSeconds(value).toMillis()
            "m" -> Duration.ofMinutes(value).toMillis()
            "h" -> Duration.ofHours(value).toMillis()
            "d" -> Duration.ofDays(value).toMillis()
            "w" -> Duration.ofDays(value * 7).toMillis()
            "M" -> Duration.ofDays(value * 30).toMillis()  // Approximate month as 30 days
            "y" -> Duration.ofDays(value * 365).toMillis()  // Approximate year as 365 days
            else -> throw IllegalArgumentException("Unknown duration unit: $unit")
        }
    }
    
    fun formatDuration(millis: Long): String {
        val duration = Duration.ofMillis(millis)
        return when {
            duration.toDays() > 0 -> "${duration.toDays()}d"
            duration.toHours() > 0 -> "${duration.toHours()}h"
            duration.toMinutes() > 0 -> "${duration.toMinutes()}m"
            else -> "${duration.toSeconds()}s"
        }
    }
}