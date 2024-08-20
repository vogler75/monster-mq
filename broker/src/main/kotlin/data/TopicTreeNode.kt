package at.rocworks.data

data class TopicTreeNode<T> (
    val children: MutableMap<String, TopicTreeNode<T>> = mutableMapOf(), // Level to Node
    val dataset: MutableSet<T> = mutableSetOf()
)