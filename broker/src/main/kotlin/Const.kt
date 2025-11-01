package at.rocworks

import java.util.logging.Level

object Const {
    const val TOPIC_KEY = "Topic"
    const val CLIENT_KEY = "Client"
    const val QOS_KEY = "QoS"
    const val COMMAND_KEY = "Command"

    const val COMMAND_STATUS = "Status"
    const val COMMAND_STATISTICS = "Statistics"
    const val COMMAND_DISCONNECT = "Disconnect"

    const val QOS2_RETRY_INTERVAL = 10L // Seconds
    const val QOS2_RETRY_COUNT = 6*10 // 10 Minutes

    const val SYS_TOPIC_NAME = "\$SYS"
    const val LOG_TOPIC_NAME = "syslogs"

    const val CONFIG_TOPIC = "<config>"    

    const val MCP_ARCHIVE_GROUP = "Default"

    const val SQLITE_DEFAULT_PATH = "sqlite"
    
    const val ANONYMOUS_USER = "Anonymous"

    var DEBUG_LEVEL: Level = Level.INFO
}