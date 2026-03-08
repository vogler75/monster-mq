package at.rocworks.schema

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class JsonSchemaValidatorTest {

    @Test
    fun schemaWithoutDollarSchemaFieldCompilesAndValidates() {
        val schemaJson = JsonObject()
            .put("type", "object")
            .put("properties", JsonObject()
                .put("temperature", JsonObject().put("type", "number"))
            )
            .put("required", JsonArray().add("temperature"))

        val validator = JsonSchemaValidator(schemaJson)

        val validResult = validator.validate("""{"temperature": 22.5}""")
        assertTrue("Should accept valid payload: ${validResult.errorDetail}", validResult.valid)

        val invalidResult = validator.validate("""{"humidity": 50}""")
        assertFalse("Should reject missing required field", invalidResult.valid)
        println("Rejected with: ${invalidResult.errorCategory} - ${invalidResult.errorDetail}")
    }

    @Test
    fun schemaWithDollarSchemaFieldCompilesAndValidates() {
        val schemaJson = JsonObject()
            .put("\$schema", "http://json-schema.org/draft-07/schema#")
            .put("type", "object")
            .put("properties", JsonObject()
                .put("temperature", JsonObject().put("type", "number"))
            )
            .put("required", JsonArray().add("temperature"))

        val validator = JsonSchemaValidator(schemaJson)

        val validResult = validator.validate("""{"temperature": 22.5}""")
        assertTrue("Should accept valid payload", validResult.valid)

        val invalidResult = validator.validate("""{"temperature": "not-a-number"}""")
        assertFalse("Should reject wrong type", invalidResult.valid)
    }

    @Test
    fun userTestSchema() {
        // The exact schema the user is testing with
        val schemaJson = JsonObject("""{
            "${'$'}schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["Value"],
            "properties": {
                "TimeMS": { "type": "number", "description": "Timestamp in milliseconds" },
                "Value": { "description": "Any type of value" },
                "Tags": {
                    "type": "object",
                    "description": "Key-value tag pairs",
                    "additionalProperties": { "type": "string" }
                }
            },
            "additionalProperties": false
        }""")

        val validator = JsonSchemaValidator(schemaJson)

        // Valid: has required "Value" field
        val valid1 = validator.validate("""{"Value": 42}""")
        assertTrue("Should accept payload with Value: ${valid1.errorDetail}", valid1.valid)

        // Valid: has Value + optional fields
        val valid2 = validator.validate("""{"Value": "hello", "TimeMS": 12345, "Tags": {"unit": "celsius"}}""")
        assertTrue("Should accept full payload: ${valid2.errorDetail}", valid2.valid)

        // Invalid: missing required "Value"
        val invalid1 = validator.validate("""{"TimeMS": 12345}""")
        assertFalse("Should reject missing Value", invalid1.valid)
        println("Missing Value rejected: ${invalid1.errorDetail}")

        // Invalid: extra property not allowed
        val invalid2 = validator.validate("""{"Value": 1, "extraField": true}""")
        assertFalse("Should reject additionalProperties", invalid2.valid)
        println("Extra field rejected: ${invalid2.errorDetail}")
    }

    @Test
    fun invalidJsonPayloadReturnsPARSE_ERROR() {
        val schemaJson = JsonObject().put("type", "object")
        val validator = JsonSchemaValidator(schemaJson)

        val result = validator.validate("not json at all")
        assertFalse(result.valid)
        assertTrue(result.errorCategory == "PARSE_ERROR")
    }
}
