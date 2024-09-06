package at.rocworks.stores

import java.time.Instant

data class SessionData(
    val cleanSession: Boolean,
    val createDate: Instant
)

interface ISessionStore {
    fun get(clientId: String): SessionData?
    fun put(clientId: String, data: SessionData)
    fun remove(clientId: String)
}