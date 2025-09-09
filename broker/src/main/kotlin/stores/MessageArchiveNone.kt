package at.rocworks.stores

import at.rocworks.data.MqttMessage
import java.time.Instant

object MessageArchiveNone : IMessageArchive {
    override fun getName(): String = "NONE"
    override fun getType(): MessageArchiveType = MessageArchiveType.NONE
    
    override fun addHistory(messages: List<MqttMessage>) {}
    
    override fun purgeOldMessages(olderThan: Instant): PurgeResult = PurgeResult(0, 0)
}