package at.rocworks.data

import java.time.LocalDateTime

data class AclRule(
    val id: String,
    val username: String,
    val topicPattern: String,
    val canSubscribe: Boolean = false,
    val canPublish: Boolean = false,
    val priority: Int = 0,
    val createdAt: LocalDateTime? = null
)