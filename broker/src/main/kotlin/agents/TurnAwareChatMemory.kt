package at.rocworks.agents

import at.rocworks.Utils
import dev.langchain4j.data.message.AiMessage
import dev.langchain4j.data.message.ChatMessage
import dev.langchain4j.data.message.SystemMessage
import dev.langchain4j.data.message.ToolExecutionResultMessage
import dev.langchain4j.data.message.UserMessage
import dev.langchain4j.memory.ChatMemory
import dev.langchain4j.store.memory.chat.ChatMemoryStore
import java.util.logging.Logger

/**
 * Chat memory implementation that evicts messages in atomic conversational turns.
 *
 * A turn consists of:
 * [UserMessage, (AiMessage with tool calls, ToolExecutionResultMessage+)*, AiMessage (final text)]
 *
 * Eviction drops entire dialogue units from the oldest history to satisfy maxMessages,
 * preventing orphaned tool calls or responses that violate strict LLM provider ordering
 * (such as Google Gemini's INVALID_ARGUMENT: function call turn followed by function response turn).
 *
 * A SystemMessage is handled like LangChain4j's MessageWindowChatMemory: at most one is kept,
 * it is always the first message, a changed system prompt replaces the old one, and it is never
 * evicted nor counted as part of a turn.
 */
class TurnAwareChatMemory(
    private val id: Any,
    private val maxMessages: Int = 40,
    private val chatMemoryStore: ChatMemoryStore? = null
) : ChatMemory {

    private val logger: Logger = Utils.getLogger(TurnAwareChatMemory::class.java)
    private val inMemoryMessages = mutableListOf<ChatMessage>()
    private val lock = Any()

    override fun id(): Any = id

    override fun add(message: ChatMessage) {
        add(listOf(message))
    }

    override fun add(vararg messages: ChatMessage) {
        add(messages.toList())
    }

    override fun add(messages: Iterable<ChatMessage>) {
        synchronized(lock) {
            val list = loadMessages()
            var changed = false
            for (message in messages) {
                if (message is SystemMessage) {
                    val existing = list.firstOrNull { it is SystemMessage }
                    if (existing == message) continue  // unchanged system prompt, nothing to store
                    list.removeAll { it is SystemMessage }
                    list.add(0, message)
                } else {
                    list.add(message)
                }
                changed = true
            }
            if (changed) evictAndStore(list)
        }
    }

    override fun set(vararg messages: ChatMessage) {
        synchronized(lock) {
            val list = messages.toMutableList()
            evictAndStore(list)
        }
    }

    override fun set(messages: Iterable<ChatMessage>) {
        synchronized(lock) {
            val list = messages.toMutableList()
            evictAndStore(list)
        }
    }

    override fun messages(): List<ChatMessage> {
        synchronized(lock) {
            val msgs = loadMessages()
            return sanitize(msgs)
        }
    }

    override fun clear() {
        synchronized(lock) {
            inMemoryMessages.clear()
            chatMemoryStore?.deleteMessages(id)
        }
    }

    private fun loadMessages(): MutableList<ChatMessage> {
        return if (chatMemoryStore != null) {
            val stored = chatMemoryStore.getMessages(id)
            if (stored != null) ArrayList(stored) else ArrayList()
        } else {
            ArrayList(inMemoryMessages)
        }
    }

    private fun evictAndStore(messages: List<ChatMessage>) {
        // The system message (last one wins) is pinned to the head and excluded from eviction.
        val systemMessage = messages.lastOrNull { it is SystemMessage }
        val conversation = messages.filter { it !is SystemMessage }
        val budget = if (systemMessage != null) maxMessages - 1 else maxMessages

        val turns = groupIntoTurns(conversation)
        var totalMessages = turns.sumOf { it.size }
        val remainingTurns = ArrayList(turns)

        // Drop complete turns from head while totalMessages > budget,
        // but preserve at least the last turn so active conversation is not wiped out.
        while (remainingTurns.size > 1 && totalMessages > budget) {
            val removedTurn = remainingTurns.removeAt(0)
            totalMessages -= removedTurn.size
            logger.fine { "TurnAwareChatMemory: evicted turn with ${removedTurn.size} messages" }
        }

        val flattened = remainingTurns.flatten()
        val sanitized = if (systemMessage != null) listOf(systemMessage) + sanitize(flattened) else sanitize(flattened)

        if (chatMemoryStore != null) {
            chatMemoryStore.updateMessages(id, sanitized)
        } else {
            inMemoryMessages.clear()
            inMemoryMessages.addAll(sanitized)
        }
    }

    companion object {
        fun withMaxMessages(maxMessages: Int): TurnAwareChatMemory {
            return TurnAwareChatMemory("default", maxMessages)
        }

        /**
         * Group messages into atomic turns (callers strip the SystemMessage first).
         * A turn starts with a UserMessage.
         * Inside a turn, AiMessages with tool calls and their ToolExecutionResultMessages
         * are kept together with the initiating UserMessage and final AiMessage response.
         */
        fun groupIntoTurns(messages: List<ChatMessage>): List<List<ChatMessage>> {
            if (messages.isEmpty()) return emptyList()

            val turns = mutableListOf<MutableList<ChatMessage>>()
            var currentTurn = mutableListOf<ChatMessage>()

            for (msg in messages) {
                if (msg is UserMessage && currentTurn.isNotEmpty()) {
                    // Check if current turn is waiting for a tool execution result
                    val lastMsg = currentTurn.lastOrNull()
                    val isWaitingForToolResult = lastMsg is AiMessage && lastMsg.hasToolExecutionRequests()
                    if (!isWaitingForToolResult) {
                        turns.add(currentTurn)
                        currentTurn = mutableListOf()
                    }
                }
                currentTurn.add(msg)

                // If msg is an AiMessage without tool requests, it marks the completion of a turn
                if (msg is AiMessage && !msg.hasToolExecutionRequests()) {
                    turns.add(currentTurn)
                    currentTurn = mutableListOf()
                }
            }

            if (currentTurn.isNotEmpty()) {
                turns.add(currentTurn)
            }

            return turns
        }

        /**
         * Sanitize messages to eliminate orphaned tool calls or tool results that cause
         * LLM providers (e.g. Gemini) to reject the payload.
         */
        fun sanitize(messages: List<ChatMessage>): List<ChatMessage> {
            if (messages.isEmpty()) return emptyList()

            val sanitized = mutableListOf<ChatMessage>()
            val activeToolCallIds = mutableSetOf<String>()

            for (msg in messages) {
                when (msg) {
                    is UserMessage -> {
                        // If there were pending tool call requests that were never fulfilled before this UserMessage,
                        // remove the abandoned AiMessage with tool calls to prevent turn ordering errors.
                        if (activeToolCallIds.isNotEmpty()) {
                            while (sanitized.isNotEmpty()) {
                                val last = sanitized.last()
                                if (last is AiMessage && last.hasToolExecutionRequests()) {
                                    sanitized.removeAt(sanitized.size - 1)
                                    break
                                } else if (last is ToolExecutionResultMessage) {
                                    sanitized.removeAt(sanitized.size - 1)
                                } else {
                                    break
                                }
                            }
                            activeToolCallIds.clear()
                        }
                        sanitized.add(msg)
                    }
                    is AiMessage -> {
                        if (msg.hasToolExecutionRequests()) {
                            val requests = msg.toolExecutionRequests()
                            val requestIds = requests.map { it.id() }.toSet()
                            activeToolCallIds.addAll(requestIds)
                        }
                        sanitized.add(msg)
                    }
                    is ToolExecutionResultMessage -> {
                        // Only include tool execution result if its corresponding tool call request was seen
                        if (activeToolCallIds.contains(msg.id())) {
                            sanitized.add(msg)
                            activeToolCallIds.remove(msg.id())
                        }
                    }
                    else -> {
                        sanitized.add(msg)
                    }
                }
            }

            return sanitized
        }
    }
}
