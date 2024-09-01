package org.example;

public class Config {
    public static String statBroker = "tcp://scada:1883";

    public static String[] publisherBroker = new String[] { "tcp://localhost:1883" };
    public static String[] subscriberBroker = new String[] { "tcp://localhost:1883" };

    //public static String[] publisherBroker = new String[] { "tcp://linux1:1883", "tcp://linux2:1883", "tcp://linux3:1883" };
    //public static String[] subscriberBroker = new String[] { "tcp://linux1:1883", "tcp://linux2:1883", "tcp://linux3:1883" };
    //public static String[] subscriberBroker = new String[] { "tcp://linux4:1883" };

    public static String topicPrefix = "test";
    public static String statisticsTopic = "monitor"; // Topic for publishing statistics

    public static int PUBLISHER_COUNT = 1;
    public static int SUBSCRIBER_COUNT = 1;

    public static boolean SUBSCRIBER_CHECK_MESSAGE_ORDER = false;

    public static double DELAY_PROCESSING_EVERY_100_MESSAGES = 10;
    public static double DELAY_PROCESSING_EVERY_10_MESSAGES = 0;
    public static double DELAY_PROCESSING_EVERY_MESSAGE = 0;

    public static int TOPIC_LEVEL_DEPTH = 100; //100;
    public static boolean RETAINED_MESSAGES = true;
    public static int QOS = 0;

}
