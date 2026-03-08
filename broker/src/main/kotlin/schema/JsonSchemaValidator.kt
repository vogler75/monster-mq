package at.rocworks.schema

import io.vertx.core.json.JsonObject
import org.everit.json.schema.Schema
import org.everit.json.schema.ValidationException
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject

/**
 * Reusable JSON Schema validator.
 *
 * Compiles a JSON Schema once and provides a thread-safe [validate] method
 * that can be called from the broker publish path, the JDBC Logger, or any
 * other component that needs payload validation.
 */
class JsonSchemaValidator(schemaJson: JsonObject) {

    val compiledSchema: Schema = try {
        val json = JSONObject(schemaJson.encode())
        // Default to draft-07 if no $schema is specified
        if (!json.has("\$schema")) {
            json.put("\$schema", "http://json-schema.org/draft-07/schema#")
        }
        SchemaLoader.builder()
            .schemaJson(json)
            .build()
            .load()
            .build()
    } catch (e: Exception) {
        throw IllegalArgumentException("Failed to compile JSON Schema: ${e.message}", e)
    }

    /**
     * Validate a JSON payload string against the compiled schema.
     *
     * @return [ValidationResult] with success/failure details.
     */
    fun validate(payload: String): ValidationResult {
        val jsonObj: JSONObject
        try {
            jsonObj = JSONObject(payload)
        } catch (e: Exception) {
            return ValidationResult(
                valid = false,
                errorCategory = "PARSE_ERROR",
                errorDetail = "Invalid JSON: ${e.message}"
            )
        }
        return validate(jsonObj)
    }

    /**
     * Validate a pre-parsed [JSONObject] against the compiled schema.
     */
    fun validate(payload: JSONObject): ValidationResult {
        return try {
            compiledSchema.validate(payload)
            ValidationResult(valid = true)
        } catch (e: ValidationException) {
            ValidationResult(
                valid = false,
                errorCategory = "SCHEMA_ERROR",
                errorDetail = e.allMessages.joinToString("; ")
            )
        }
    }
}

/**
 * Result of a JSON Schema validation.
 */
data class ValidationResult(
    val valid: Boolean,
    val errorCategory: String? = null,
    val errorDetail: String? = null
)
