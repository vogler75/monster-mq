package org.example;

public class Config {
    public static String statBroker = "tcp://localhost:1883";

    public static String[] publisherBroker = new String[] { "tcp://localhost:1883" };
    public static String[] subscriberBroker = new String[] { "tcp://localhost:1883" };

    //public static String[] publisherBroker = new String[] { "tcp://linux1:1883", "tcp://linux2:1883", "tcp://linux3:1883" };
    //public static String[] subscriberBroker = new String[] { "tcp://linux1:1883", "tcp://linux2:1883", "tcp://linux3:1883" };
    //public static String[] subscriberBroker = new String[] { "tcp://linux4:1883" };

    public static String topicPrefix = "test";
    public static String statisticsTopic = "test/monitor"; // Topic for publishing statistics

    public static int PUBLISHER_COUNT = 10;
    public static int SUBSCRIBER_COUNT = 10;

    public static int SUBSCRIBER_QOS = 0;
    public static int PUBLISHER_QOS = 0;

    public static boolean SUBSCRIBER_CHECK_MESSAGE_ORDER = true;
    public static boolean SUBSCRIBER_WILDCARD_SUBSCRIPTION = true;
    public static boolean SUBSCRIBER_SUBSCRIBE_BROADCAST = false;
    public static boolean SUBSCRIBER_CLEANSESSION = true;
    public static boolean SUBSCRIBER_EXIT = false;

    public static double DELAY_PROCESSING_EVERY_100_MESSAGES = 0;
    public static double DELAY_PROCESSING_EVERY_10_MESSAGES = 0;
    public static double DELAY_PROCESSING_EVERY_MESSAGE = 1000;

    public static int TOPIC_LEVEL_DEPTH = 10; //100;
    public static boolean RETAINED_MESSAGES = true;

}
