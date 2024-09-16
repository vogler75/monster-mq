package at.rocworks.data

enum class TopicTreeType {
    LOCAL,
    DISTRIBUTED
}

interface ITopicTree<K, V> {
    fun getType(): TopicTreeType

    fun add(topicName: String)
    fun add(topicName: String, key: K?, value: V?)

    fun addAll(topicNames: List<String>) {
        topicNames.forEach(::add)
    }

    fun del(topicName: String)
    fun del(topicName: String, key: K?)

    fun delAll(topicNames: List<String>) {
        topicNames.forEach(::del)
    }

    /*
       The given topicName will be matched with potential wildcard topics of the tree (tree contains wildcard topics),
       and the data of all matching topics will be returned
        */
    fun findDataOfTopicName(topicName: String): List<Pair<K, V>>

    /*
       The given topicName will be matched with potential wildcard topics of the tree (tree contains wildcard topics),
       it will stop at the first matching topic and return true, if none is found it will return false
        */
    fun isTopicNameMatching(topicName: String): Boolean

    /*
       The given topicName can contain wildcards and this will be matched with the tree topics without wildcards
        */
    fun findMatchingTopicNames(topicName: String): List<String>
    fun findMatchingTopicNames(topicName: String, callback: (String)->Boolean)

}