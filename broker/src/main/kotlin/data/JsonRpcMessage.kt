package at.rocworks.data

import io.vertx.core.json.JsonObject

/**
 * JSON-RPC 2.0 Request message format
 */
data class JsonRpcRequest(
    val jsonrpc: String = "2.0",
    val method: String,
    val params: Map<String, Any>? = null,
    val id: String
) {
    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("jsonrpc", jsonrpc)
            .put("method", method)
            .put("id", id)

        if (params != null) {
            obj.put("params", JsonObject(params))
        }

        return obj
    }

    companion object {
        fun fromJsonObject(json: JsonObject): JsonRpcRequest? {
            return try {
                val jsonrpc = json.getString("jsonrpc", "2.0")
                val method = json.getString("method") ?: return null
                val id = json.getString("id") ?: return null
                val paramsObj = json.getJsonObject("params")
                val params = paramsObj?.map?.toMap() as? Map<String, Any>

                JsonRpcRequest(jsonrpc, method, params, id)
            } catch (e: Exception) {
                null
            }
        }
    }
}

/**
 * JSON-RPC 2.0 Response message format
 */
data class JsonRpcResponse(
    val jsonrpc: String = "2.0",
    val result: Any? = null,
    val error: JsonRpcError? = null,
    val id: String
) {
    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("jsonrpc", jsonrpc)
            .put("id", id)

        if (error != null) {
            obj.put("error", error.toJsonObject())
        } else {
            obj.put("result", result)
        }

        return obj
    }

    companion object {
        fun success(id: String, result: Any?): JsonRpcResponse {
            return JsonRpcResponse(result = result, id = id)
        }

        fun error(id: String, code: Int, message: String, data: Any? = null): JsonRpcResponse {
            return JsonRpcResponse(error = JsonRpcError(code, message, data), id = id)
        }

        fun parseError(id: String): JsonRpcResponse {
            return error(id, -32700, "Parse error")
        }

        fun invalidRequest(id: String): JsonRpcResponse {
            return error(id, -32600, "Invalid Request")
        }

        fun methodNotFound(id: String): JsonRpcResponse {
            return error(id, -32601, "Method not found")
        }

        fun invalidParams(id: String): JsonRpcResponse {
            return error(id, -32602, "Invalid params")
        }

        fun internalError(id: String, message: String = "Internal error"): JsonRpcResponse {
            return error(id, -32603, message)
        }
    }
}

/**
 * JSON-RPC 2.0 Error object
 */
data class JsonRpcError(
    val code: Int,
    val message: String,
    val data: Any? = null
) {
    fun toJsonObject(): JsonObject {
        val obj = JsonObject()
            .put("code", code)
            .put("message", message)

        if (data != null) {
            obj.put("data", data)
        }

        return obj
    }
}
