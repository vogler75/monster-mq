package at.rocworks.stores

import at.rocworks.data.TopicTree
import io.vertx.core.Future

data class ArchiveGroup(
    val name: String,
    val topicFilter: List<String>,
    val retainedOnly: Boolean,
    val lastValStore: IMessageStore?,
    val lastValReady: Future<Void>,
    val archiveStore: IMessageArchive?,
    val archiveReady: Future<Void>,
    val filterTree: TopicTree<Boolean, Boolean> = TopicTree()
) {
    init {
        topicFilter.forEach { filterTree.add(it, key=true, value=true) }
    }
}
