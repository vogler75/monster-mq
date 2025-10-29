package at.rocworks.genai

/**
 * Response from a GenAI provider
 *
 * @param response The AI-generated text response
 * @param model The specific model that was used to generate the response
 * @param error Optional error message if the request failed
 */
data class GenAiResponse(
    val response: String,
    val model: String? = null,
    val error: String? = null
) {
    companion object {
        /**
         * Create an error response
         */
        fun error(message: String, model: String? = null) = GenAiResponse(
            response = "",
            error = message,
            model = model
        )

        /**
         * Create a successful response
         */
        fun success(text: String, model: String? = null) = GenAiResponse(
            response = text,
            model = model
        )
    }
}
