package at.rocworks.genai

/**
 * Request to a GenAI provider
 *
 * @param prompt The main prompt/question for the AI
 * @param context Optional additional context to provide to the AI
 * @param docs Optional list of documentation file paths to include as context
 */
data class GenAiRequest(
    val prompt: String,
    val context: String? = null,
    val docs: List<String>? = null
)
