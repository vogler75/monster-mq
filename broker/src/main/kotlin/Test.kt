package at.rocworks

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
    topics.forEach { tree.add(TopicName(it)) }

    val tests = listOf("#", "a/#", "a/+/b", "a/b/+", "a/b/a", "x", "x/y")
    tests.forEach {
        println("Test: $it: "+tree.findMatchingTopicNames(TopicName(it)).joinToString(" | "))
    }
}