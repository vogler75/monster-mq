package at.rocworks.data

import java.io.Serializable

class MqttSubscription(val clientId: MqttClientId, val topicName: MqttTopicName): Serializable
