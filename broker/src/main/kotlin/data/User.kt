package at.rocworks.data

import java.time.LocalDateTime

data class User(
    val username: String,
    val passwordHash: String,
    val enabled: Boolean = true,
    val canSubscribe: Boolean = true,
    val canPublish: Boolean = true,
    val isAdmin: Boolean = false,
    val createdAt: LocalDateTime? = null,
    val updatedAt: LocalDateTime? = null
)