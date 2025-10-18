package at.rocworks.stores

import at.rocworks.data.BrokerMessage
import at.rocworks.data.PurgeResult
import java.time.Instant

object MessageArchiveNone : IMessageArchive {
    override fun getName(): String = "NONE"
    override fun getType(): MessageArchiveType = MessageArchiveType.NONE

    override fun addHistory(messages: List<BrokerMessage>) {}

    override fun purgeOldMessages(olderThan: Instant): PurgeResult = PurgeResult(0, 0)

    override fun dropStorage(): Boolean = true

    override fun getConnectionStatus(): Boolean = true // NONE store is always "connected"

    override suspend fun tableExists(): Boolean = true // NONE archive has no table requirements

    override suspend fun createTable(): Boolean = true // NONE archive requires no table creation
}