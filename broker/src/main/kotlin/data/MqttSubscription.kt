package at.rocworks.data

import java.io.Serializable

class MqttSubscription(val clientId: String, val topicName: String): Serializable
