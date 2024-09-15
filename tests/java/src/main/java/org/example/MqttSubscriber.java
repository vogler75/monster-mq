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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class MqttSubscriber {

    static AtomicInteger runningTasks = new AtomicInteger(0);

    public static void main(String[] args) {
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            System.out.println(args.length);
            int startNr = 1;
            if (args.length > 0) startNr = Integer.parseInt(args[0]);
            for (int i = 0; i < Config.SUBSCRIBER_COUNT; i++) {
                int nr = startNr + i;
                // use a atomic variable to keep track of running tasks
                runningTasks.incrementAndGet();
                executor.submit(() -> test(nr));
                Thread.sleep(100);
                /*
                if (i % 1000 == 0) {
                    System.out.println("Started subscriber " + nr);
                    // Check if executor has running tasks
                    while (runningTasks.get()>0) {
                        Thread.sleep(100);
                    }
                }
                 */
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static void test(int nr) {
        String testClientId = "subscriber_" + nr; // UUID.randomUUID();
        String statClientId = "subscriber_stats_" + nr; // UUID.randomUUID(); // Second client for statistics

        String TOPIC = Config.topicPrefix + "/" + nr;

        try {
            // Create the second MQTT client instance for statistics
            MqttClient statClient = Config.SUBSCRIBER_EXIT ? null : getMqttClient(statClientId);

            // Create an MQTT client instance
            var broker = Config.subscriberBroker[nr % Config.subscriberBroker.length];
            MqttClient client = new MqttClient(broker, testClientId, new MemoryPersistence());

            // Define the MQTT connection options
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(Config.SUBSCRIBER_CLEANSESSION);

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
            //System.out.println("Connected successfully "+nr);

            // Subscribe to the topic
            if (Config.SUBSCRIBER_WILDCARD_SUBSCRIPTION) {
                client.subscribe(TOPIC+"/#", Config.SUBSCRIBER_QOS);
                if (Config.SUBSCRIBER_SUBSCRIBE_BROADCAST)
                    client.subscribe(Config.topicPrefix+"/broadcast", Config.SUBSCRIBER_QOS);
            } else {
                int topicNr1=0, topicNr2=0, topicNr3=0;
                int amount=(int)(Math.pow(Config.TOPIC_LEVEL_DEPTH, 3));
                for (int i=0; i<amount; i++) {
                    topicNr3++;
                    if (topicNr3 == Config.TOPIC_LEVEL_DEPTH) {
                        topicNr3 = 0;
                        topicNr2++;
                        if (topicNr2 == Config.TOPIC_LEVEL_DEPTH) {
                            topicNr2 = 0;
                            topicNr1++;
                            if (topicNr1 == Config.TOPIC_LEVEL_DEPTH) {
                                topicNr1 = 0;
                            }
                        }
                    }
                    String topic = TOPIC + "/" + topicNr1 + "/" + topicNr2 + "/" + topicNr3;
                    //System.out.println("Subscribe to "+topic);
                    client.subscribe(topic, Config.SUBSCRIBER_QOS);
                }
            }

            //System.out.println("Subscribed to topic: " + TOPIC);

            // Start the network loop
            Instant lastTime = Instant.now();
            int lastCounter = 0;

            if (Config.SUBSCRIBER_EXIT) {
                client.disconnect();
                if (statClient != null) statClient.disconnect();
            }
            else while (true) {
                // Optionally add some logic to exit the loop if necessary
                Thread.sleep(1000);

                Instant currentTime = Instant.now();
                Duration diff = Duration.between(lastTime, currentTime);

                int messageCounter = callback.messageCounter;
                int messages = messageCounter - lastCounter;

                double throughput = (double) messages / diff.getSeconds();
                var statisticsMessage = "Messages " + messageCounter + " / " + messages + " / " + throughput + " / " + diff.getSeconds();
                //System.out.println(statisticsMessage);

                lastCounter = messageCounter;
                lastTime = currentTime;

                // Publish statistics to the second broker
                MqttMessage statsMsg = new MqttMessage(statisticsMessage.getBytes());
                statsMsg.setQos(0);
                statClient.publish(Config.statisticsTopic+"/subscriber/instance_"+nr, statsMsg);
            }
        } catch (MqttException | InterruptedException e) {
            System.out.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
        }
        runningTasks.decrementAndGet();
        //System.out.println("Exiting subscriber "+nr);
    }

    private static MqttClient getMqttClient(String statClientId) throws MqttException {
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
        return statClient;
    }
}

