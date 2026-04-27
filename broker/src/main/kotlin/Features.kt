package at.rocworks

object Features {
    const val OpcUa = "OpcUa"
    const val OpcUaServer = "OpcUaServer"
    const val MqttClient = "MqttClient"
    const val Kafka = "Kafka"
    const val Nats = "Nats"
    const val Redis = "Redis"
    const val Telegram = "Telegram"
    const val WinCCOa = "WinCCOa"
    const val WinCCUa = "WinCCUa"
    const val Plc4x = "Plc4x"
    const val Neo4j = "Neo4j"
    const val JdbcLogger = "JdbcLogger"
    const val InfluxDBLogger = "InfluxDBLogger"
    const val TimeBaseLogger = "TimeBaseLogger"
    const val SparkplugB = "SparkplugB"
    const val FlowEngine = "FlowEngine"
    const val Agents = "Agents"
    const val GenAi = "GenAi"
    const val Mcp = "Mcp"
    const val SchemaPolicy = "SchemaPolicy"
    const val TopicNamespace = "TopicNamespace"
    const val DeviceImportExport = "DeviceImportExport"

    val all: List<String> = listOf(
        OpcUa, OpcUaServer, MqttClient, Kafka, Nats, Redis, Telegram,
        WinCCOa, WinCCUa, Plc4x, Neo4j, JdbcLogger, InfluxDBLogger, TimeBaseLogger,
        SparkplugB, FlowEngine, Agents,
        GenAi, Mcp, SchemaPolicy, TopicNamespace, DeviceImportExport
    )
}
