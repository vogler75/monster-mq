package org.example;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

public class MqttSubscriber {

    public static void main(String[] args) {
        System.out.println(args.length);
        int startNr = 1;
        if (args.length > 0) startNr = Integer.parseInt(args[0]);
        for (int i=0; i<Config.SUBSCRIBER_COUNT; i++) {
            int nr=startNr+i;
            new Thread(() -> test(nr)).start();
        }
    }

    static void test(int nr) {
        String testClientId = "subscriber_" + UUID.randomUUID();
        String statClientId = "statistics_" + UUID.randomUUID(); // Second client for statistics

        String TOPIC = Config.topicPrefix + "/" + nr + "/#";

        try {
            // Create the second MQTT client instance for statistics
            MqttClient statClient = new MqttClient(Config.statBroker, statClientId, new MemoryPersistence());

            // Define callback for the second client
            statClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to broker lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // No incoming messages expected for this client in this scenario
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });
            statClient.connect();

            // Create an MQTT client instance
            MqttClient client = new MqttClient(Config.testBroker, testClientId, new MemoryPersistence());

            // Define the MQTT connection options
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);

            // Set up the callback functions
            var callback = new MqttCallback() {

                public volatile int messageCounter = 0;
                private int lastMessage = 0;

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    messageCounter++;

                    if (Config.SUBSCRIBER_CHECK_MESSAGE_ORDER) {
                        try {
                            var thisMessage = Integer.parseInt(new String(message.getPayload()));
                            if (lastMessage + 1 != thisMessage) {
                                System.out.println("Got wrong number " + lastMessage + " vs " + thisMessage);
                            }
                            lastMessage = thisMessage;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    // Not needed in this scenario
                }
            };
            client.setCallback(callback);

            // Connect to the MQTT broker
            client.connect(options);
            System.out.println("Connected successfully "+nr);

            // Subscribe to the topic
            client.subscribe(TOPIC);
            //System.out.println("Subscribed to topic: " + TOPIC);

            // Start the network loop
            Instant lastTime = Instant.now();
            int lastCounter = 0;

            while (true) {
                // Optionally add some logic to exit the loop if necessary
                Thread.sleep(1000);

                Instant currentTime = Instant.now();
                Duration diff = Duration.between(lastTime, currentTime);

                int messageCounter = callback.messageCounter;
                int messages = messageCounter - lastCounter;

                double throughput = (double) messages / diff.getSeconds();
                var statisticsMessage = "Messages " + messageCounter + " / " + lastCounter + " / " + throughput + " / " + diff.getSeconds();
                //System.out.println(statisticsMessage);

                lastCounter = messageCounter;
                lastTime = currentTime;

                // Publish statistics to the second broker
                MqttMessage statsMsg = new MqttMessage(statisticsMessage.getBytes());
                statsMsg.setQos(0);
                statClient.publish(Config.statisticsTopic+"/subscriber/instance_"+nr, statsMsg);

            }

        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

