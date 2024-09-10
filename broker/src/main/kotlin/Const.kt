package at.rocworks

import java.util.logging.Level

object Const {
    const val GLOBAL_CLIENT_NAMESPACE = "CNS"
    const val GLOBAL_DISTRIBUTOR_NAMESPACE = "DNS"

    const val GLOBAL_SUBSCRIPTION_TABLE_NAMESPACE = "ST"
    const val GLOBAL_CLIENT_TABLE_NAMESPACE = "CT"

    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val COMMAND_KEY = "Command"

    const val COMMAND_STATUS = "Status"

    var DEBUG_LEVEL: Level = Level.INFO
}