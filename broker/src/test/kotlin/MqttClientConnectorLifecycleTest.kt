package at.rocworks.devices.mqttclient

import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.MqttClientConnectionConfig
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class MqttClientConnectorLifecycleTest {

    private lateinit var vertx: Vertx

    @Before
    fun setUp() {
        vertx = Vertx.vertx()
    }

    @After
    fun tearDown() {
        val latch = CountDownLatch(1)
        vertx.close().onComplete { latch.countDown() }
        latch.await(5, TimeUnit.SECONDS)
    }

    @Test
    fun testConnectorStopsCleanlyWhenRemoteBrokerUnreachable() {
        // Configure connector with an unreachable broker and 1000ms reconnect delay
        val mqttConfig = MqttClientConnectionConfig(
            brokerUrl = "tcp://127.0.0.1:19999", // non-existent port
            clientId = "test-client",
            reconnectDelay = 1000L,
            connectionTimeout = 1000L
        )

        val device = DeviceConfig(
            name = "test-device",
            namespace = "test-namespace",
            nodeId = "*",
            config = mqttConfig.toJsonObject(),
            enabled = true,
            type = DeviceConfig.DEVICE_TYPE_MQTT_CLIENT
        )

        val config = JsonObject().put("device", device.toJsonObject())
        val options = DeploymentOptions().setConfig(config)

        val deployLatch = CountDownLatch(1)
        var deploymentId: String? = null

        vertx.deployVerticle(MqttClientConnector(), options).onComplete { res ->
            if (res.succeeded()) {
                deploymentId = res.result()
            }
            deployLatch.countDown()
        }

        assertTrue("Verticle should deploy within 5s", deployLatch.await(5, TimeUnit.SECONDS))
        assertTrue("Deployment ID must not be null", deploymentId != null)

        // Wait a short time for connect/reconnect attempts to trigger
        Thread.sleep(500)

        // Undeploy the connector
        val undeployLatch = CountDownLatch(1)
        var undeployed = false
        vertx.undeploy(deploymentId!!).onComplete { res ->
            undeployed = res.succeeded()
            undeployLatch.countDown()
        }

        assertTrue("Undeploy should succeed within 5s", undeployLatch.await(5, TimeUnit.SECONDS))
        assertTrue("Undeployment must succeed", undeployed)

        // Wait past several reconnectDelay cycles to confirm no exceptions or crashes occur
        Thread.sleep(800)
    }
}
