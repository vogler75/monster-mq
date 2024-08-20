package at.rocworks

import at.rocworks.codecs.MqttTopicName

fun main(args: Array<String>) {
    val tree = TopicTree()

    val topics = listOf(
        "a/a/a",
        "a/a/b",
        "a/b/a",
        "a/b/b",
        "b/a/a",
        "b/b/a"
        )
    topics.forEach { tree.add(MqttTopicName(it)) }

    val tests = listOf("#", "a/#", "a/+/b", "a/b/+", "a/b/a", "x", "x/y")
    tests.forEach {
        println("Test: $it: "+tree.findMatchingTopicNames(MqttTopicName(it)).joinToString(" | "))
    }
}