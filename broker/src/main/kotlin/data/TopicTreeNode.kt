package at.rocworks.data

import io.vertx.core.impl.ConcurrentHashSet
import java.util.concurrent.ConcurrentHashMap

data class TopicTreeNode<T> (
    val children: ConcurrentHashMap<String, TopicTreeNode<T>> = ConcurrentHashMap(), // Level to Node
    val dataset: ConcurrentHashSet<T> = ConcurrentHashSet()
)