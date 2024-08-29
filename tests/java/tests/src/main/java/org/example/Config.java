package org.example;

public class Config {
    public static String testBroker = "tcp://nuc1:1883";
    public static String statBroker = "tcp://scada:1883";

    public static String topicPrefix = "test";
    public static String statisticsTopic = "monitor"; // Topic for publishing statistics

    public static int PUBLISHER_COUNT = 1000;
    public static int SUBSCRIBER_COUNT = 1000;
    public static boolean SUBSCRIBER_CHECK_MESSAGE_ORDER = true;

    public static double DELAY_PROCESSING = 1000;
    public static boolean RETAINED_MESSAGES = false;
    public static int QOS = 0;
}