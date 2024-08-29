package org.example;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;

import java.util.UUID;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

public class MqttPublisher {

    public static void main(String[] args) {
        int startNr = 1;
        if (args.length > 0) startNr = Integer.parseInt(args[0]);
        for (int i=0; i<Config.PUBLISHER_COUNT; i++) {
            int nr=startNr+i;
            new Thread(() -> test(nr)).start();
        }
    }

    public static void test(int nr) {
        String testClientId = "publisher_" + UUID.randomUUID();
        String statClientId = "statistics_" + UUID.randomUUID(); // Second client for statistics

        try {
            // Create the first MQTT client instance
            MqttClient testClient = new MqttClient(Config.testBroker, testClientId, new MemoryPersistence());

            // Define callback for the first client
            testClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to broker1 lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println("Received `" + new String(message.getPayload()) + "` from `" + topic + "` topic");
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            // Create the second MQTT client instance for statistics
            MqttClient statClient = new MqttClient(Config.statBroker, statClientId, new MemoryPersistence());

            // Define callback for the second client
            statClient.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to broker2 lost: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // No incoming messages expected for this client in this scenario
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            // Connect both clients to their respective brokers
            testClient.connect();
            statClient.connect();

            // Wait until both clients are connected
            while (!testClient.isConnected() || !statClient.isConnected()) {
                TimeUnit.SECONDS.sleep(1);
            }

            System.out.println("Start "+nr);
            int messageCounter = 0;
            LocalDateTime lastTime = LocalDateTime.now();
            int lastCounter = 0;
            int topicNr1 = 0, topicNr2 = 0, topicNr3 = 0;

            while (messageCounter < 1_000_000) {
                topicNr3++;
                if (topicNr3 == 100) {
                    topicNr3 = 0;
                    topicNr2++;
                    if (topicNr2 == 100) {
                        topicNr2 = 0;
                        topicNr1++;
                        if (topicNr1 == 100) {
                            topicNr1 = 0;
                        }
                    }
                }

                String topic = Config.topicPrefix +"/" + nr + "/" + topicNr1 + "/" + topicNr2 + "/" + topicNr3;
                messageCounter++;
                lastCounter++;

                MqttMessage message = new MqttMessage(Integer.toString(messageCounter).getBytes());
                message.setQos(Config.QOS);
                message.setRetained(Config.RETAINED_MESSAGES);

                testClient.publish(topic, message);

                if (messageCounter % 100 == 0) {
                    LocalDateTime currentTime = LocalDateTime.now();
                    Duration diff = Duration.between(lastTime, currentTime);
                    if (diff.getSeconds() >= 1) {
                        double throughput = lastCounter / (double) diff.getSeconds();
                        String statisticsMessage = "Messages " + messageCounter + " / " + lastCounter + " / " + Math.round(throughput) + " / " + diff.getSeconds();
                        //System.out.println(statisticsMessage);
                        lastCounter = 0;
                        lastTime = currentTime;

                        // Publish statistics to the second broker
                        MqttMessage statsMsg = new MqttMessage(statisticsMessage.getBytes());
                        statsMsg.setQos(Config.QOS);
                        statClient.publish(Config.statisticsTopic+"/publisher/instance_"+nr, statsMsg);
                    }

                    TimeUnit.MILLISECONDS.sleep((long) Config.DELAY_PROCESSING);
                }
            }

            System.out.println("Done.");
            TimeUnit.SECONDS.sleep(1);
            System.out.println("Disconnecting...");
            testClient.disconnect();
            statClient.disconnect();
            TimeUnit.SECONDS.sleep(1);
            System.out.println("Ended.");

        } catch (MqttException me) {
            me.printStackTrace();
        } catch (InterruptedException ie) {
            ie.printStackTrace();
        }
    }
}
