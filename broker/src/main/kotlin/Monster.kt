package at.rocworks

import at.rocworks.data.MqttMessage
import at.rocworks.data.MqttMessageCodec
import at.rocworks.data.MqttSubscription
import at.rocworks.data.MqttSubscriptionCodec
import at.rocworks.stores.*
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.core.spi.cluster.ClusterManager
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess

class Monster(args: Array<String>) {
    private val logger: Logger = Logger.getLogger("Monster")

    private val configFileName: String
    private var config: JsonObject = JsonObject()

    private val postgres = object {
        var url: String = ""
        var user: String = ""
        var pass: String = ""
    }

    companion object {
        private var isClustered = false
        fun isClustered() = isClustered
    }

    init {
        Utils.initLogging()

        // Clustering
        isClustered = args.find { it == "-cluster" } != null

        // Config file
        val configFileIndex = args.indexOf("-config")
        configFileName = if (configFileIndex != -1 && configFileIndex + 1 < args.size) {
            args[configFileIndex + 1]
        } else {
            System.getenv("GATEWAY_CONFIG") ?: "config.yaml"
        }

        args.indexOf("-log").let {
            if (it != -1) {
                val level = Level.parse(args[it + 1])
                println("Log Level [$level]")
                Const.DEBUG_LEVEL = level
            }
        }

        logger.info("Cluster: ${isClustered()}")

        val builder = Vertx.builder()
        if (isClustered())
            clusterSetup(builder)
        else
            localSetup(builder)
    }

    private fun getConfigRetriever(vertx: Vertx): ConfigRetriever {
        logger.info("Monster config file: $configFileName")
        return ConfigRetriever.create(
            vertx,
            ConfigRetrieverOptions().addStore(
                ConfigStoreOptions()
                    .setType("file")
                    .setFormat("yaml")
                    .setConfig(JsonObject().put("path", configFileName))
            )
        )
    }

    private fun getConfigAndStart(vertx: Vertx, clusterManager: ClusterManager?) {
        getConfigRetriever(vertx).config.onComplete { it ->
            if (it.succeeded()) {
                this.config = it.result()
                val pg = config.getJsonObject("Postgres", JsonObject())
                postgres.url = pg.getString("Url", "jdbc:postgresql://localhost:5432/postgres")
                postgres.user = pg.getString("User", "system")
                postgres.pass = pg.getString("Pass", "manager")
                startMonster(vertx, clusterManager)
            } else {
                logger.severe("Config loading failed: ${it.cause()}")
            }
        }
    }

    private fun localSetup(builder: VertxBuilder) {
        val vertx = builder.build()
        getConfigAndStart(vertx, null)
    }

    private fun clusterSetup(builder: VertxBuilder) {
        //val hazelcastConfig = ConfigUtil.loadConfig()
        //hazelcastConfig.setClusterName("MonsterMQ")
        val clusterManager = HazelcastClusterManager()

        //val clusterManager = ZookeeperClusterManager()
        //val clusterManager = InfinispanClusterManager()
        //val clusterManager = IgniteClusterManager();

        builder.withClusterManager(clusterManager)
        builder.buildClustered().onComplete { res: AsyncResult<Vertx?> ->
            if (res.succeeded() && res.result() != null) {
                val vertx = res.result()!!
                getConfigAndStart(vertx, clusterManager)
            } else {
                logger.severe("Vertx building failed: ${res.cause()}")
            }
        }
    }

    private fun startMonster(vertx: Vertx, clusterManager: ClusterManager?) {
        val usePort = config.getInteger("Port", 1883)
        val useSsl = config.getBoolean("SSL", false)
        val useWs = config.getBoolean("WS", false)
        val useTcp = config.getBoolean("TCP", true)

        val retainedMessages = config.getJsonObject("RetainedMessages", JsonObject())
        val lastValueMessages = config.getJsonObject("LastValueMessages", JsonObject())
        val lastValueFilter = lastValueMessages.getJsonArray("TopicFilter", JsonArray()).toList().map { it as String }

        val retainedMessage = object {
            val storeType = MessageStoreType.valueOf(retainedMessages.getString("StoreType", "MEMORY"))
            val archiveType = MessageArchiveType.valueOf(retainedMessages.getString("ArchiveType", "NONE"))
        }

        val lastValueMessage = object {
            val storeType = MessageStoreType.valueOf(lastValueMessages.getString("StoreType", "NONE"))
            val archiveType = MessageArchiveType.valueOf(lastValueMessages.getString("ArchiveType", "NONE"))
        }

        logger.info("Port [$usePort] SSL [$useSsl] WS [$useWs] TCP [$useTcp]")

        logger.info("RetainedMessageStoreType [${retainedMessage.storeType}]")
        logger.info("LastValueMessageStoreType [${lastValueMessage.storeType}]")

        vertx.eventBus().registerDefaultCodec(MqttMessage::class.java, MqttMessageCodec())
        vertx.eventBus().registerDefaultCodec(MqttSubscription::class.java, MqttSubscriptionCodec())

        getSessionStore(vertx).onSuccess { sessionStore ->
            val retainedStore = getMessageStore(vertx, "RetainedMessages", retainedMessage.storeType, clusterManager)
            val lastValueStore = getMessageStore(vertx, "LastValueMessages", lastValueMessage.storeType, clusterManager)

            val retainedArch = getMessageArchive(vertx, "RetainedArchive", retainedMessage.archiveType)
            val lastValueArch = getMessageArchive(vertx, "LastValueArchive", lastValueMessage.archiveType)

            Future.succeededFuture<Unit>()
                .compose { retainedStore }
                .compose { lastValueStore }
                .compose { retainedArch }
                .compose { lastValueArch }
                .onFailure {
                    logger.severe("Message store creation failed: ${it.message}")
                    exitProcess(-1)
                }
                .onComplete {
                    logger.info("Message stores are ready.")
                    val messageHandler = MessageHandler(
                        retainedStore.result()!!,
                        retainedArch.result(),
                        lastValueFilter,
                        lastValueStore.result(),
                        lastValueArch.result()
                    )
                    val sessionHandler = SessionHandler(sessionStore)

                    val distributor = getDistributor(sessionHandler, messageHandler)
                    val servers = listOfNotNull(
                        if (useTcp) MqttServer(usePort, useSsl, false, distributor) else null,
                        if (useWs) MqttServer(usePort, useSsl, true, distributor) else null
                    )

                    Future.succeededFuture<String>()
                        .compose { vertx.deployVerticle(messageHandler) }
                        .compose { vertx.deployVerticle(sessionHandler) }
                        .compose { vertx.deployVerticle(distributor) }
                        .compose { Future.all(servers.map { vertx.deployVerticle(it) }) }
                        .onFailure {
                            logger.severe("Startup error: ${it.message}")
                            exitProcess(-1)
                        }
                        .onComplete {
                            logger.info("The Monster is ready.")
                        }
                }
        }.onFailure {
            logger.severe("Session store creation failed: ${it.message}")
        }
    }

    private fun getDistributor(sessionHandler: SessionHandler, messageHandler: MessageHandler): Distributor {
        val kafka = config.getJsonObject("Kafka", JsonObject())
        val kafkaEnabled = kafka.getBoolean("Enabled", false)
        val distributor = if (!kafkaEnabled) DistributorVertx(sessionHandler, messageHandler)
        else {
            val kafkaServers = kafka.getString("Servers", "")
            val kafkaTopic = kafka.getString("Topic", "monster")
            DistributorKafka(sessionHandler, messageHandler, kafkaServers, kafkaTopic)
        }
        return distributor
    }

    private fun getSessionStore(vertx: Vertx): Future<ISessionStore> {
        val promise = Promise.promise<ISessionStore>()
        val sessionStoreType = SessionStoreType.valueOf(
            config.getString("SessionStoreType", "MEMORY")
        )
        when (sessionStoreType) {
            SessionStoreType.POSTGRES -> {
                val store = SessionStorePostgres(postgres.url, postgres.user, postgres.pass)
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
            }
        }
        return promise.future()
    }

    private fun getMessageStore(vertx: Vertx, name: String, storeType: MessageStoreType, clusterManager: ClusterManager?): Future<IMessageStore?> {
        val promise = Promise.promise<IMessageStore?>()
        when (storeType) {
            MessageStoreType.NONE -> promise.complete()
            MessageStoreType.MEMORY -> {
                val store = MessageStoreMemory(name)
                vertx.deployVerticle(store).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
            }
            MessageStoreType.POSTGRES -> {
                val store = MessageStorePostgres(
                    name = name,
                    url = postgres.url,
                    username = postgres.user,
                    password = postgres.pass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
            }
            MessageStoreType.HAZELCAST -> {
                if (clusterManager is HazelcastClusterManager) {
                    val store = MessageStoreHazelcast(name, clusterManager.hazelcastInstance)
                    vertx.deployVerticle(store).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
                } else {
                    logger.severe("Cannot create Hazelcast message store with this cluster manager.")
                    exitProcess(-1)
                }
            }
        }
        return promise.future()
    }

    private fun getMessageArchive(vertx: Vertx, name: String, storeType: MessageArchiveType): Future<IMessageArchive?> {
        val promise = Promise.promise<IMessageArchive?>()
        when (storeType) {
            MessageArchiveType.NONE -> promise.complete()
            MessageArchiveType.POSTGRES -> {
                val store = MessageArchivePostgres(
                    name = name,
                    url = postgres.url,
                    username = postgres.user,
                    password = postgres.pass
                )
                val options: DeploymentOptions = DeploymentOptions().setThreadingModel(ThreadingModel.WORKER)
                vertx.deployVerticle(store, options).onSuccess { promise.complete(store) }.onFailure { promise.fail(it) }
            }
        }
        return promise.future()
    }
}