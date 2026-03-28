# GraalVM JavaScript Capabilities Analysis

## Current Architecture

### How Proxies Work

MonsterMQ already uses the **Proxy Pattern** to expose Java/Kotlin functionality to JavaScript:

```kotlin
// 1. Define a Kotlin class with methods
class ConsoleProxy(private val instanceName: String? = null) {
    @Suppress("unused")  // Kotlin doesn't see usage, but JS does
    fun log(vararg messages: Any?) {
        // Java code executes here
        logger.info(messages.joinToString(" "))
    }
}

// 2. Add to script bindings
val consoleProxy = ConsoleProxy(instanceName)
bindings.putMember("console", consoleProxy)

// 3. JavaScript can call it directly
// console.log("Hello")  // Calls consoleProxy.log()
```

### Existing Proxies

1. **OutputsProxy** - `outputs.send(portName, value)`
2. **ConsoleProxy** - `console.log()`, `console.warn()`, `console.error()`
3. **DatabasesProxy** - `dbs.get(nodeId).execute(sql, args)`

**Key Insight:** All methods are **synchronous/blocking**. JavaScript waits for Java method to complete.

## What's Possible with `allowAllAccess(true)`

### ✅ Currently Available

1. **Direct Java Class Access**
   ```javascript
   // JavaScript can call Java classes directly
   const ArrayList = Java.type('java.util.ArrayList');
   const list = new ArrayList();
   list.add("item");
   ```

2. **HTTP Requests via Java**
   ```javascript
   const HttpClient = Java.type('java.net.http.HttpClient');
   const HttpRequest = Java.type('java.net.http.HttpRequest');
   const URI = Java.type('java.net.URI');

   const client = HttpClient.newHttpClient();
   const request = HttpRequest.newBuilder()
       .uri(URI.create("http://localhost:8080/graphql"))
       .POST(HttpRequest.BodyPublishers.ofString('{"query":"..."}'))
       .header("Content-Type", "application/json")
       .build();

   const response = client.send(request, HttpResponse.BodyHandlers.ofString());
   const body = response.body();
   ```

3. **File I/O**
   ```javascript
   const Files = Java.type('java.nio.file.Files');
   const Paths = Java.type('java.nio.file.Paths');
   const content = Files.readString(Paths.get("file.txt"));
   ```

4. **Threading (with caution)**
   ```javascript
   const Thread = Java.type('java.lang.Thread');
   Thread.sleep(1000); // Sleep for 1 second
   ```

### ❌ NOT Available

1. **fetch API** - Not included in GraalVM JS
2. **XMLHttpRequest** - Not included in GraalVM JS
3. **Browser APIs** - DOM, localStorage, etc.
4. **Node.js APIs** - require(), fs, http modules

### 🎯 Recommendation: Use Proxy Pattern

**Why NOT expose raw Java classes?**
- Security risk - users could access anything
- Complex syntax - `Java.type()` is verbose
- No type safety or validation
- Harder to document and maintain

**Why USE Proxy Pattern?**
- ✅ Controlled API surface
- ✅ Type-safe and validated
- ✅ Easy to document
- ✅ Clean JavaScript syntax
- ✅ Can add business logic/checks

## Proposed Generalized Architecture

### Option 1: Domain-Specific Proxies (RECOMMENDED)

Create separate proxy objects for different domains:

```kotlin
// In FlowScriptEngine.execute()
bindings.putMember("genai", GenAiProxy(genAiProvider))
bindings.putMember("workflows", WorkflowsProxy(vertx))
bindings.putMember("mqtt", MqttProxy(vertx))
bindings.putMember("http", HttpProxy())
```

**JavaScript Usage:**
```javascript
// Clean, domain-specific APIs
let ai = genai.generate({ prompt: "..." });
let flows = workflows.list();
let devices = mqtt.getClients();
let response = http.get("https://api.example.com/data");
```

**Advantages:**
- Each domain has specific, documented methods
- Type-safe parameters
- Easy to extend with new domains
- Clear separation of concerns

### Option 2: Generic API Object

Single `api` object that routes to different services:

```kotlin
bindings.putMember("api", ApiProxy(services))
```

**JavaScript Usage:**
```javascript
let result = api.call("genai.generate", { prompt: "..." });
let flows = api.call("workflows.list", {});
```

**Disadvantages:**
- Less type-safe
- Harder to document
- String-based routing is error-prone

### Option 3: GraphQL Proxy

Direct GraphQL access:

```kotlin
bindings.putMember("graphql", GraphQLProxy(graphQLServer))
```

**JavaScript Usage:**
```javascript
let result = graphql.query(`
  query {
    genai {
      generate(prompt: "help me") {
        response
        model
      }
    }
  }
`);
```

**Disadvantages:**
- Requires users to know GraphQL syntax
- More verbose
- No type checking

## Recommended Implementation Plan

### Phase 1: Core Infrastructure Proxies

```javascript
// HTTP client for external APIs
http.get(url, options)
http.post(url, body, options)
http.request(method, url, options)

// MQTT operations
mqtt.publish(topic, payload, qos, retain)
mqtt.subscribe(topic)  // One-time subscribe
mqtt.getLastValue(topic)  // From last value store

// Workflows (read-only for now)
workflows.list()
workflows.get(name)
workflows.getInstance(name)
```

### Phase 2: AI & Advanced Features

```javascript
// GenAI
genai.generate({ prompt, context, docs })

// Future: Vector search
vectors.search(query, limit)
vectors.store(id, embedding, metadata)
```

### Phase 3: Write Operations (Careful!)

```javascript
// With proper validation and permissions
workflows.create(definition)
workflows.start(instanceName)
workflows.stop(instanceName)

mqtt.publish(topic, payload)  // Already safe
```

## Security Considerations

1. **Validation**: All proxy methods must validate inputs
2. **Sandboxing**: Don't expose destructive operations
3. **Rate Limiting**: Prevent abuse (e.g., infinite HTTP requests)
4. **Timeout**: Set max execution time for blocking calls
5. **Resource Limits**: Limit memory, CPU usage

## Example: HTTP Proxy Implementation

```kotlin
class HttpProxy {
    private val client = java.net.http.HttpClient.newHttpClient()

    @Suppress("unused")
    fun get(url: String, options: Any? = null): HttpResponse {
        return try {
            // Validate URL
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                return HttpResponse(false, 0, "", "Invalid URL protocol")
            }

            // Build request
            val requestBuilder = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(url))
                .timeout(java.time.Duration.ofSeconds(30))
                .GET()

            // Add headers from options
            if (options != null && options is Map<*, *>) {
                val headers = options["headers"] as? Map<*, *>
                headers?.forEach { (key, value) ->
                    requestBuilder.header(key.toString(), value.toString())
                }
            }

            val request = requestBuilder.build()
            val response = client.send(
                request,
                java.net.http.HttpResponse.BodyHandlers.ofString()
            )

            HttpResponse(
                success = true,
                status = response.statusCode(),
                body = response.body(),
                error = null
            )

        } catch (e: Exception) {
            HttpResponse(false, 0, "", e.message ?: "HTTP request failed")
        }
    }

    @Suppress("unused")
    fun post(url: String, body: Any?, options: Any? = null): HttpResponse {
        // Similar implementation
    }

    data class HttpResponse(
        val success: Boolean,
        val status: Int,
        val body: String,
        val error: String?
    )
}
```

**JavaScript Usage:**
```javascript
let response = http.get("https://api.weather.com/current", {
    headers: {
        "Authorization": "Bearer " + flow.apiKey
    }
});

if (response.success) {
    let data = JSON.parse(response.body);
    outputs.send("weather", data);
} else {
    console.error("HTTP error:", response.error);
}
```

## Conclusion

**YES, we can generalize this!**

1. ✅ **Technically Possible** - GraalVM with `allowAllAccess(true)` supports it
2. ✅ **Pattern Proven** - DatabasesProxy shows complex proxies work
3. ✅ **Synchronous Model Works** - Blocking is fine for workflows
4. ✅ **Extensible** - Easy to add new proxies

**Recommended Approach:**
- Use **domain-specific proxy objects** (Option 1)
- Start with: `genai`, `http`, `mqtt` (read-only)
- Add more as needed: `workflows`, `vectors`, etc.
- Document each thoroughly
- Validate all inputs
- Set reasonable timeouts/limits

**Next Steps:**
1. Implement GenAiProxy (already planned)
2. Implement HttpProxy for external APIs
3. Enhance MqttProxy (currently workflows only have indirect access)
4. Add to documentation with examples
5. Consider which GraphQL APIs to expose
