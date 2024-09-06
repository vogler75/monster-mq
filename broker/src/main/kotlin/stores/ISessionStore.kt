package at.rocworks.stores

import at.rocworks.data.MqttSession

interface ISessionStore {
    fun get(clientId: String): MqttSession?
    fun add(session: MqttSession)
    fun del(clientId: String)
}