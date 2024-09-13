package at.rocworks

import java.util.logging.Level

object Const {
    const val GLOBAL_CLIENT_NAMESPACE = "CNS"
    const val GLOBAL_DISTRIBUTOR_NAMESPACE = "DNS"

    const val GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE = "ST"
    const val GLOBAL_CLIENT_TABLE_NAMESPACE = "CT"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val QOS_KEY = "QoS"
    const val COMMAND_KEY = "Command"

    const val COMMAND_STATUS = "Status"
    const val COMMAND_DISCONNECT = "Disconnect"

    const val QOS2_RETRY_INTERVAL = 10L // Seconds
    const val QOS2_RETRY_COUNT = 6*10 // 10 Minutes

    var DEBUG_LEVEL: Level = Level.INFO
}