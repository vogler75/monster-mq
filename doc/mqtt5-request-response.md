# MQTT v5.0 Request/Response Pattern

## Overview

MQTT v5.0 introduces built-in support for the request/response pattern through two properties:
- **Response Topic** (Property 8): Where the requester expects the response
- **Correlation Data** (Property 9): Token to correlate request with response

This pattern enables synchronous-like communication over the asynchronous MQTT protocol.

## How It Works

1. **Requester** sends a message with:
   - `Response Topic`: Topic where requester is listening for responses
   - `Correlation Data`: Unique token to identify this specific request
   - Optional: `Content Type` to indicate request payload format

2. **Responder** receives message, processes it, and publishes response to:
   - Topic specified in `Response Topic`
   - Includes same `Correlation Data` to correlate response with request
   - Optional: `Content Type` for response format

3. **Requester** receives response on `Response Topic`:
   - Matches `Correlation Data` to identify which request this response belongs to
   - Processes response

## Implementation Status

MonsterMQ fully supports the Request/Response pattern:
- ✅ Response Topic forwarding (Phase 3)
- ✅ Correlation Data forwarding (Phase 3)
- ✅ Content Type forwarding (Phase 3)
- ✅ Properties preserved through broker routing

## Python Example

### Requester (Client making request)

```python
import paho.mqtt.client as mqtt
import uuid
import time

BROKER_HOST = "localhost"
BROKER_PORT = 1883
REQUEST_TOPIC = "service/temperature/query"
RESPONSE_TOPIC = "response/client123"  # Unique per client

# Store pending requests
pending_requests = {}

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with reason code: {reason_code}")
    # Subscribe to our response topic
    client.subscribe(RESPONSE_TOPIC)
    print(f"Subscribed to response topic: {RESPONSE_TOPIC}")

def on_message(client, userdata, msg):
    """Handle response messages"""
    # Extract correlation data
    correlation_data = None
    if hasattr(msg, 'properties') and msg.properties:
        correlation_data = msg.properties.CorrelationData
    
    if correlation_data:
        correlation_id = correlation_data.decode('utf-8')
        if correlation_id in pending_requests:
            print(f"\nReceived response for request {correlation_id}")
            print(f"Response: {msg.payload.decode('utf-8')}")
            del pending_requests[correlation_id]
        else:
            print(f"Unknown correlation ID: {correlation_id}")

def send_request(client, location):
    """Send temperature query request"""
    # Generate unique correlation ID
    correlation_id = str(uuid.uuid4())
    
    # Create properties for request
    properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    properties.ResponseTopic = RESPONSE_TOPIC
    properties.CorrelationData = correlation_id.encode('utf-8')
    properties.ContentType = "text/plain"
    
    # Send request
    payload = f"query:{location}"
    client.publish(
        REQUEST_TOPIC,
        payload,
        qos=1,
        properties=properties
    )
    
    pending_requests[correlation_id] = {
        'location': location,
        'timestamp': time.time()
    }
    
    print(f"Sent request {correlation_id}: {payload}")
    return correlation_id

# Main
client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="requester_client",
    protocol=mqtt.MQTTv5
)

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)
client.loop_start()

# Wait for connection
time.sleep(1)

# Send requests
request1 = send_request(client, "Vienna")
time.sleep(0.5)
request2 = send_request(client, "Berlin")

# Wait for responses
time.sleep(5)

# Cleanup
client.disconnect()
client.loop_stop()

print("\nRequest/Response demo completed")
```

### Responder (Service processing requests)

```python
import paho.mqtt.client as mqtt
import random

BROKER_HOST = "localhost"
BROKER_PORT = 1883
REQUEST_TOPIC = "service/temperature/query"

# Simulated temperature data
TEMPERATURE_DATA = {
    "Vienna": 22.5,
    "Berlin": 18.3,
    "London": 15.8,
    "Paris": 20.1
}

def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with reason code: {reason_code}")
    # Subscribe to request topic
    client.subscribe(REQUEST_TOPIC)
    print(f"Subscribed to request topic: {REQUEST_TOPIC}")

def on_message(client, userdata, msg):
    """Handle request messages"""
    print(f"\nReceived request: topic={msg.topic}, payload={msg.payload.decode('utf-8')}")
    
    # Extract Response Topic and Correlation Data
    response_topic = None
    correlation_data = None
    content_type = None
    
    if hasattr(msg, 'properties') and msg.properties:
        response_topic = msg.properties.ResponseTopic
        correlation_data = msg.properties.CorrelationData
        content_type = getattr(msg.properties, 'ContentType', None)
    
    if not response_topic:
        print("No Response Topic - cannot send response")
        return
    
    # Process request
    payload_str = msg.payload.decode('utf-8')
    if payload_str.startswith("query:"):
        location = payload_str[6:]  # Remove "query:" prefix
        
        # Get temperature (or use random if not found)
        if location in TEMPERATURE_DATA:
            temperature = TEMPERATURE_DATA[location]
        else:
            temperature = random.uniform(10.0, 30.0)
        
        response_payload = f"Temperature in {location}: {temperature:.1f}°C"
        
        # Create response properties
        response_props = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
        if correlation_data:
            response_props.CorrelationData = correlation_data
        response_props.ContentType = "text/plain"
        
        # Send response
        client.publish(
            response_topic,
            response_payload,
            qos=1,
            properties=response_props
        )
        
        print(f"Sent response to {response_topic}")
        if correlation_data:
            print(f"Correlation Data: {correlation_data.decode('utf-8')}")

# Main
client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="responder_service",
    protocol=mqtt.MQTTv5
)

client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, keepalive=60)

print("Temperature Service is running...")
print("Press Ctrl+C to stop")

try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\nShutting down...")
    client.disconnect()
```

## Running the Example

1. **Start the broker:**
   ```bash
   cd broker
   ./run.sh  # or run.bat on Windows
   ```

2. **Start the responder (terminal 1):**
   ```bash
   cd tests
   python3 response_service.py
   ```

3. **Run the requester (terminal 2):**
   ```bash
   cd tests
   python3 request_client.py
   ```

## Expected Output

**Responder Output:**
```
Connected with reason code: 0
Subscribed to request topic: service/temperature/query
Temperature Service is running...

Received request: topic=service/temperature/query, payload=query:Vienna
Sent response to response/client123
Correlation Data: a1b2c3d4-e5f6-7g8h-9i0j-k1l2m3n4o5p6

Received request: topic=service/temperature/query, payload=query:Berlin
Sent response to response/client123
Correlation Data: f9e8d7c6-b5a4-3210-1234-567890abcdef
```

**Requester Output:**
```
Connected with reason code: 0
Subscribed to response topic: response/client123
Sent request a1b2c3d4-e5f6-7g8h-9i0j-k1l2m3n4o5p6: query:Vienna
Sent request f9e8d7c6-b5a4-3210-1234-567890abcdef: query:Berlin

Received response for request a1b2c3d4-e5f6-7g8h-9i0j-k1l2m3n4o5p6
Response: Temperature in Vienna: 22.5°C

Received response for request f9e8d7c6-b5a4-3210-1234-567890abcdef
Response: Temperature in Berlin: 18.3°C

Request/Response demo completed
```

## Best Practices

### 1. Unique Response Topics
Each requester should use a unique response topic:
```python
# Good: Include client ID or UUID
RESPONSE_TOPIC = f"response/{client_id}"

# Bad: Shared response topic
RESPONSE_TOPIC = "responses"  # Multiple clients would receive each other's responses
```

### 2. Correlation Data Format
Use UUIDs or other unique identifiers:
```python
import uuid
correlation_id = str(uuid.uuid4())
properties.CorrelationData = correlation_id.encode('utf-8')
```

### 3. Timeout Handling
Implement timeouts for requests:
```python
REQUEST_TIMEOUT = 5.0  # seconds

def check_timeouts():
    current_time = time.time()
    for req_id, req_data in list(pending_requests.items()):
        if current_time - req_data['timestamp'] > REQUEST_TIMEOUT:
            print(f"Request {req_id} timed out")
            del pending_requests[req_id]
```

### 4. QoS Considerations
- Use QoS 1 for both requests and responses to ensure delivery
- QoS 2 can be used for exactly-once delivery but adds overhead

### 5. Content Type
Specify content types for better interoperability:
```python
properties.ContentType = "application/json"  # JSON payload
properties.ContentType = "text/plain"        # Plain text
properties.ContentType = "application/xml"   # XML payload
```

## Advanced Patterns

### Multiple Response Topics
For load balancing across multiple requester instances:
```python
# Requester 1
RESPONSE_TOPIC = "response/pool/instance1"

# Requester 2
RESPONSE_TOPIC = "response/pool/instance2"

# Responder round-robins between instances
```

### Async/Await Pattern
Wrap request/response in async functions:
```python
import asyncio

async def async_request(client, topic, payload, timeout=5.0):
    correlation_id = str(uuid.uuid4())
    future = asyncio.get_event_loop().create_future()
    
    pending_requests[correlation_id] = future
    
    properties = mqtt.Properties(mqtt.PacketTypes.PUBLISH)
    properties.ResponseTopic = RESPONSE_TOPIC
    properties.CorrelationData = correlation_id.encode('utf-8')
    
    client.publish(topic, payload, qos=1, properties=properties)
    
    try:
        response = await asyncio.wait_for(future, timeout=timeout)
        return response
    except asyncio.TimeoutError:
        del pending_requests[correlation_id]
        raise TimeoutError(f"Request {correlation_id} timed out")

# Usage
response = await async_request(client, REQUEST_TOPIC, "query:Vienna")
```

## Troubleshooting

### Response not received
1. Verify requester subscribed to response topic before sending request
2. Check correlation data matches between request and response
3. Ensure responder extracts Response Topic correctly
4. Verify QoS level (use QoS 1 minimum)

### Wrong response received
1. Use unique correlation IDs (UUIDs recommended)
2. Verify response topic is unique per requester
3. Check for message ordering issues with QoS 0

### Performance issues
1. Use persistent sessions to avoid resubscribing
2. Implement connection pooling for multiple requesters
3. Consider message batching for high-volume scenarios
4. Use QoS 1 instead of QoS 2 when possible

## References

- MQTT v5.0 Specification: Section 4.10 (Request/Response)
- MonsterMQ MQTT5 Implementation Plan: Phase 3 (Properties)
- Paho Python MQTT v5 Documentation: Properties
