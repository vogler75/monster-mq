package at.rocworks.devices.mqttclient

import at.rocworks.stores.DeviceConfig
import at.rocworks.stores.devices.MqttClientAddress
import at.rocworks.stores.devices.MqttClientConnectionConfig
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

class MqttClientConnectorProtocolTest {

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

    private fun waitUndeploy(deploymentId: String?) {
        if (deploymentId == null) return
        val latch = CountDownLatch(1)
        vertx.undeploy(deploymentId).onComplete { latch.countDown() }
        latch.await(5, TimeUnit.SECONDS)
    }

    private fun readPacket(input: InputStream): ByteArray {
        val header = input.read()
        if (header == -1) throw IllegalStateException("EOF reading header")
        var multiplier = 1
        var remainingLength = 0
        var digit: Int
        do {
            digit = input.read()
            if (digit == -1) throw IllegalStateException("EOF reading remaining length")
            remainingLength += (digit and 0x7F) * multiplier
            multiplier *= 128
        } while ((digit and 0x80) != 0)

        val packet = ByteArray(1 + remainingLength)
        packet[0] = header.toByte()
        var offset = 1
        var needed = remainingLength
        while (needed > 0) {
            val count = input.read(packet, offset, needed)
            if (count == -1) throw IllegalStateException("EOF reading payload")
            offset += count
            needed -= count
        }
        return packet
    }

    @Test
    fun testMqtt5ConnectAndSubscribeWithNoLocalAndRetainAsPublished() {
        val server = ServerSocket(0)
        val port = server.localPort

        val connectProtocolVersion = AtomicInteger(-1)
        val subscribeOptionByte = AtomicInteger(-1)
        val subscribeLatch = CountDownLatch(1)

        val serverThread = Thread {
            try {
                val socket = server.accept()
                val input = socket.getInputStream()
                val output = socket.getOutputStream()

                // Read CONNECT packet
                val connectPacket = readPacket(input)
                assertEquals(0x10, connectPacket[0].toInt() and 0xFF)
                // Protocol name is "MQTT" (len 4), protocol level is at index 7
                val protoLevel = connectPacket[7].toInt() and 0xFF
                connectProtocolVersion.set(protoLevel)

                // Send MQTT 5 CONNACK: 0x20, remaining length 3, flags 0, reason code 0, prop len 0
                output.write(byteArrayOf(0x20.toByte(), 0x03.toByte(), 0x00.toByte(), 0x00.toByte(), 0x00.toByte()))
                output.flush()

                // Read SUBSCRIBE packet
                val subPacket = readPacket(input)
                assertEquals(0x82, subPacket[0].toInt() and 0xFF)
                // The last byte of the packet is subscription options
                val subOptions = subPacket.last().toInt() and 0xFF
                subscribeOptionByte.set(subOptions)

                // Send SUBACK
                output.write(byteArrayOf(0x90.toByte(), 0x04.toByte(), subPacket[2], subPacket[3], 0x00.toByte(), 0x00.toByte()))
                output.flush()

                subscribeLatch.countDown()
                socket.close()
            } catch (e: Exception) {
                e.printStackTrace()
            } finally {
                server.close()
            }
        }
        serverThread.start()

        val address = MqttClientAddress(
            mode = MqttClientAddress.MODE_SUBSCRIBE,
            remoteTopic = "sensor/temp",
            localTopic = "local/sensor/temp"
        )

        val mqttConfig = MqttClientConnectionConfig(
            brokerUrl = "tcp://127.0.0.1:$port",
            clientId = "test-client-v5",
            protocolVersion = 5,
            addresses = listOf(address),
            reconnectDelay = 1000L,
            connectionTimeout = 2000L
        )

        val device = DeviceConfig(
            name = "test-v5",
            namespace = "test",
            nodeId = "*",
            config = mqttConfig.toJsonObject(),
            enabled = true,
            type = DeviceConfig.DEVICE_TYPE_MQTT_CLIENT
        )

        val deployLatch = CountDownLatch(1)
        var deploymentId: String? = null
        vertx.deployVerticle(MqttClientConnector(), DeploymentOptions().setConfig(JsonObject().put("device", device.toJsonObject())))
            .onComplete { res ->
                deploymentId = res.result()
                deployLatch.countDown()
            }

        assertTrue("Verticle deployed", deployLatch.await(5, TimeUnit.SECONDS))
        assertTrue("Subscribe packet received", subscribeLatch.await(5, TimeUnit.SECONDS))

        // Assert MQTT 5 protocol was used
        assertEquals(5, connectProtocolVersion.get())

        // Subscription options in MQTT 5:
        // bit 2 = No Local (0x04)
        // bit 3 = Retain As Published (0x08)
        val opts = subscribeOptionByte.get()
        assertTrue("No Local should be set", (opts and 0x04) != 0)
        assertTrue("Retain As Published should be set", (opts and 0x08) != 0)

        waitUndeploy(deploymentId)
    }

    @Test
    fun testMqtt5FallbackToMqtt3WhenServerReturnsV3Connack() {
        val server = ServerSocket(0)
        val port = server.localPort

        val firstConnectProtocol = AtomicInteger(-1)
        val secondConnectProtocol = AtomicInteger(-1)
        val secondConnectLatch = CountDownLatch(1)

        val serverThread = Thread {
            try {
                // First connection attempt (MQTT 5)
                val socket1 = server.accept()
                val input1 = socket1.getInputStream()
                val output1 = socket1.getOutputStream()

                val connect1 = readPacket(input1)
                firstConnectProtocol.set(connect1[7].toInt() and 0xFF)

                // Broker only supports 3.1.1: reply with 3.1.1 CONNACK (len 2, return code 1: unacceptable protocol)
                output1.write(byteArrayOf(0x20.toByte(), 0x02.toByte(), 0x00.toByte(), 0x01.toByte()))
                output1.flush()
                socket1.close()

                // Second connection attempt (Fallback to MQTT 3.1.1)
                val socket2 = server.accept()
                val input2 = socket2.getInputStream()
                val output2 = socket2.getOutputStream()

                val connect2 = readPacket(input2)
                secondConnectProtocol.set(connect2[7].toInt() and 0xFF)

                // Accept connection with MQTT 3.1.1 CONNACK (return code 0: success)
                output2.write(byteArrayOf(0x20.toByte(), 0x02.toByte(), 0x00.toByte(), 0x00.toByte()))
                output2.flush()

                secondConnectLatch.countDown()
                socket2.close()
            } catch (e: Exception) {
                e.printStackTrace()
            } finally {
                server.close()
            }
        }
        serverThread.start()

        val mqttConfig = MqttClientConnectionConfig(
            brokerUrl = "tcp://127.0.0.1:$port",
            clientId = "test-fallback-client",
            protocolVersion = 5,
            reconnectDelay = 1000L,
            connectionTimeout = 2000L
        )

        val device = DeviceConfig(
            name = "test-fallback",
            namespace = "test",
            nodeId = "*",
            config = mqttConfig.toJsonObject(),
            enabled = true,
            type = DeviceConfig.DEVICE_TYPE_MQTT_CLIENT
        )

        val deployLatch = CountDownLatch(1)
        var deploymentId: String? = null
        vertx.deployVerticle(MqttClientConnector(), DeploymentOptions().setConfig(JsonObject().put("device", device.toJsonObject())))
            .onComplete { res ->
                deploymentId = res.result()
                deployLatch.countDown()
            }

        assertTrue("Verticle deployed", deployLatch.await(5, TimeUnit.SECONDS))
        assertTrue("Second connection attempt received after fallback", secondConnectLatch.await(10, TimeUnit.SECONDS))

        assertEquals("First connect must be MQTT 5", 5, firstConnectProtocol.get())
        assertEquals("Second connect must be MQTT 3.1.1 (protocol 4)", 4, secondConnectProtocol.get())

        waitUndeploy(deploymentId)
    }

    @Test
    fun testMqtt5SharedSubscriptionDoesNotSetNoLocal() {
        val server = ServerSocket(0)
        val port = server.localPort

        val subscribeOptionByte = AtomicInteger(-1)
        val subscribeLatch = CountDownLatch(1)

        val serverThread = Thread {
            try {
                val socket = server.accept()
                val input = socket.getInputStream()
                val output = socket.getOutputStream()

                // Read CONNECT packet
                readPacket(input)

                // Send MQTT 5 CONNACK
                output.write(byteArrayOf(0x20.toByte(), 0x03.toByte(), 0x00.toByte(), 0x00.toByte(), 0x00.toByte()))
                output.flush()

                // Read SUBSCRIBE packet
                val subPacket = readPacket(input)
                val subOptions = subPacket.last().toInt() and 0xFF
                subscribeOptionByte.set(subOptions)

                // Send SUBACK
                output.write(byteArrayOf(0x90.toByte(), 0x04.toByte(), subPacket[2], subPacket[3], 0x00.toByte(), 0x00.toByte()))
                output.flush()

                subscribeLatch.countDown()
                socket.close()
            } catch (e: Exception) {
                e.printStackTrace()
            } finally {
                server.close()
            }
        }
        serverThread.start()

        val address = MqttClientAddress(
            mode = MqttClientAddress.MODE_SUBSCRIBE,
            remoteTopic = "\$share/group1/sensor/temp",
            localTopic = "local/sensor/temp"
        )

        val mqttConfig = MqttClientConnectionConfig(
            brokerUrl = "tcp://127.0.0.1:$port",
            clientId = "test-shared-v5",
            protocolVersion = 5,
            addresses = listOf(address),
            reconnectDelay = 1000L,
            connectionTimeout = 2000L
        )

        val device = DeviceConfig(
            name = "test-shared",
            namespace = "test",
            nodeId = "*",
            config = mqttConfig.toJsonObject(),
            enabled = true,
            type = DeviceConfig.DEVICE_TYPE_MQTT_CLIENT
        )

        val deployLatch = CountDownLatch(1)
        var deploymentId: String? = null
        vertx.deployVerticle(MqttClientConnector(), DeploymentOptions().setConfig(JsonObject().put("device", device.toJsonObject())))
            .onComplete { res ->
                deploymentId = res.result()
                deployLatch.countDown()
            }

        assertTrue("Verticle deployed", deployLatch.await(5, TimeUnit.SECONDS))
        assertTrue("Subscribe packet received", subscribeLatch.await(5, TimeUnit.SECONDS))

        val opts = subscribeOptionByte.get()
        assertFalse("No Local must NOT be set on shared subscription", (opts and 0x04) != 0)
        assertTrue("Retain As Published should be set", (opts and 0x08) != 0)

        waitUndeploy(deploymentId)
    }
}
