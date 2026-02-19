#!/usr/bin/env python3
"""
MQTT v5.0 Request/Response Pattern Test

Tests the Request/Response pattern using MQTT v5 properties:
- Response Topic (property ID 8): Topic for response messages
- Correlation Data (property ID 9): Application-specific correlation identifier

Per MQTT v5.0 spec 4.10: Request/Response is a common pattern in client/server
interactions. MQTT v5.0 includes two properties to support this:
- Response Topic: UTF-8 Encoded String indicating where to send the response
- Correlation Data: Binary Data used to correlate the Request and Response

Test Scenarios:
1. Simple request-response: Client sends request with responseTopicand correlationData
2. Multiple concurrent requests: Different correlationData for each request
3. Response without request: Validate handling of unsolicited responses
4. Round-trip timing: Measure request-response latency
"""

import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import time
import sys
import json
import uuid

# Configuration
BROKER_HOST = "localhost"
BROKER_PORT = 1883
REQUEST_TOPIC = "service/temperature/request"
RESPONSE_TOPIC_BASE = "service/temperature/response"

# Test state
requests_sent = []
responses_received = []
service_requests_received = []
connections = {}

def on_connect(client, userdata, flags, rc, properties=None):
    """Handle connection callback"""
    client_name = userdata
    print(f"[{client_name}] Connected rc={rc}")
    connections[client_name] = True

def on_message_requester(client, userdata, msg):
    """Handle response messages for requester"""
    payload = json.loads(msg.payload.decode('utf-8'))
    
    # Extract correlation data from properties
    correlation_data = None
    if hasattr(msg, 'properties') and msg.properties:
        if hasattr(msg.properties, 'CorrelationData'):
            correlation_data = msg.properties.CorrelationData
    
    print(f"[Requester] Received response:")
    print(f"  Topic: {msg.topic}")
    print(f"  Correlation Data: {correlation_data}")
    print(f"  Payload: {payload}")
    
    responses_received.append({
        'topic': msg.topic,
        'correlation_data': correlation_data,
        'payload': payload,
        'timestamp': time.time()
    })

def on_message_responder(client, userdata, msg):
    """Handle request messages for service responder"""
    payload = json.loads(msg.payload.decode('utf-8'))
    
    # Extract response topic and correlation data
    response_topic = None
    correlation_data = None
    if hasattr(msg, 'properties') and msg.properties:
        if hasattr(msg.properties, 'ResponseTopic'):
            response_topic = msg.properties.ResponseTopic
        if hasattr(msg.properties, 'CorrelationData'):
            correlation_data = msg.properties.CorrelationData
    
    print(f"[Responder] Received request:")
    print(f"  Topic: {msg.topic}")
    print(f"  Response Topic: {response_topic}")
    print(f"  Correlation Data: {correlation_data}")
    print(f"  Payload: {payload}")
    
    service_requests_received.append({
        'topic': msg.topic,
        'response_topic': response_topic,
        'correlation_data': correlation_data,
        'payload': payload,
        'timestamp': time.time()
    })
    
    # Send response if response_topic is provided
    if response_topic and correlation_data:
        # Simulate processing
        time.sleep(0.1)
        
        # Create response payload
        response_payload = {
            'status': 'success',
            'sensor_id': payload.get('sensor_id'),
            'temperature': 22.5,
            'unit': 'celsius',
            'timestamp': time.time()
        }
        
        # Create response properties with correlation data
        response_props = Properties(PacketTypes.PUBLISH)
        response_props.CorrelationData = correlation_data
        
        # Publish response
        client.publish(
            response_topic,
            json.dumps(response_payload),
            qos=1,
            properties=response_props
        )
        print(f"[Responder] Sent response to {response_topic}")

def on_disconnect(client, userdata, flags, rc, properties=None):
    """Handle disconnect for MQTT v5"""
    client_name = userdata
    print(f"[{client_name}] Disconnected rc={rc}")

def on_publish(client, userdata, mid, reason_code=None, properties=None):
    """Handle publish acknowledgment"""
    pass  # Silent publish ACK

def test_simple_request_response():
    """Test 1: Simple request-response pattern"""
    print("\n" + "="*70)
    print("TEST 1: Simple Request-Response Pattern")
    print("="*70)
    
    # Reset state
    requests_sent.clear()
    responses_received.clear()
    service_requests_received.clear()
    connections.clear()
    
    # Create unique response topic for this requester
    response_topic = f"{RESPONSE_TOPIC_BASE}/{uuid.uuid4().hex[:8]}"
    
    # Create requester (client)
    requester = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="requester1",
                           protocol=mqtt.MQTTv5,
                           userdata="Requester")
    requester.on_connect = on_connect
    requester.on_message = on_message_requester
    requester.on_disconnect = on_disconnect
    requester.on_publish = on_publish
    
    requester.connect(BROKER_HOST, BROKER_PORT, 60)
    requester.loop_start()
    time.sleep(0.5)
    
    # Subscribe to response topic
    requester.subscribe(response_topic, qos=1)
    print(f"[Requester] Subscribed to response topic: {response_topic}")
    time.sleep(0.5)
    
    # Create responder (service)
    responder = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="responder1",
                           protocol=mqtt.MQTTv5,
                           userdata="Responder")
    responder.on_connect = on_connect
    responder.on_message = on_message_responder
    responder.on_disconnect = on_disconnect
    
    responder.connect(BROKER_HOST, BROKER_PORT, 60)
    responder.loop_start()
    time.sleep(0.5)
    
    # Subscribe to request topic
    responder.subscribe(REQUEST_TOPIC, qos=1)
    print(f"[Responder] Subscribed to request topic: {REQUEST_TOPIC}")
    time.sleep(0.5)
    
    # Send request with Response Topic and Correlation Data
    print("\n[Requester] Sending request...")
    correlation_id = uuid.uuid4().bytes
    request_payload = {
        'sensor_id': 'temp_sensor_001',
        'action': 'read_temperature'
    }
    
    request_props = Properties(PacketTypes.PUBLISH)
    request_props.ResponseTopic = response_topic
    request_props.CorrelationData = correlation_id
    
    request_time = time.time()
    result = requester.publish(
        REQUEST_TOPIC,
        json.dumps(request_payload),
        qos=1,
        properties=request_props
    )
    result.wait_for_publish()
    
    requests_sent.append({
        'correlation_data': correlation_id,
        'payload': request_payload,
        'timestamp': request_time
    })
    
    # Wait for request processing and response
    time.sleep(1)
    
    # Verify request was received by responder
    if len(service_requests_received) == 0:
        print("✗ TEST 1 FAILED: Responder did not receive request")
        requester.loop_stop()
        requester.disconnect()
        responder.loop_stop()
        responder.disconnect()
        return False
    
    # Verify response was received by requester
    if len(responses_received) == 0:
        print("✗ TEST 1 FAILED: Requester did not receive response")
        requester.loop_stop()
        requester.disconnect()
        responder.loop_stop()
        responder.disconnect()
        return False
    
    # Validate correlation
    request_sent = requests_sent[0]
    response_recv = responses_received[0]
    
    if request_sent['correlation_data'] != response_recv['correlation_data']:
        print(f"✗ TEST 1 FAILED: Correlation data mismatch")
        print(f"  Sent: {request_sent['correlation_data'].hex()}")
        print(f"  Received: {response_recv['correlation_data'].hex()}")
        success = False
    else:
        # Calculate round-trip time
        rtt = response_recv['timestamp'] - request_sent['timestamp']
        print(f"\n✓ TEST 1 PASSED: Request-Response pattern working")
        print(f"  Correlation ID matched: {correlation_id.hex()}")
        print(f"  Round-trip time: {rtt*1000:.2f}ms")
        print(f"  Response payload: {response_recv['payload']}")
        success = True
    
    # Cleanup
    requester.loop_stop()
    requester.disconnect()
    responder.loop_stop()
    responder.disconnect()
    time.sleep(0.5)
    
    return success

def test_concurrent_requests():
    """Test 2: Multiple concurrent requests with different correlation IDs"""
    print("\n" + "="*70)
    print("TEST 2: Concurrent Requests with Different Correlation IDs")
    print("="*70)
    
    # Reset state
    requests_sent.clear()
    responses_received.clear()
    service_requests_received.clear()
    connections.clear()
    
    # Create unique response topic for this requester
    response_topic = f"{RESPONSE_TOPIC_BASE}/{uuid.uuid4().hex[:8]}"
    
    # Create requester (client)
    requester = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="requester2",
                           protocol=mqtt.MQTTv5,
                           userdata="Requester")
    requester.on_connect = on_connect
    requester.on_message = on_message_requester
    requester.on_disconnect = on_disconnect
    requester.on_publish = on_publish
    
    requester.connect(BROKER_HOST, BROKER_PORT, 60)
    requester.loop_start()
    time.sleep(0.5)
    
    # Subscribe to response topic
    requester.subscribe(response_topic, qos=1)
    time.sleep(0.5)
    
    # Create responder (service)
    responder = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                           client_id="responder2",
                           protocol=mqtt.MQTTv5,
                           userdata="Responder")
    responder.on_connect = on_connect
    responder.on_message = on_message_responder
    responder.on_disconnect = on_disconnect
    
    responder.connect(BROKER_HOST, BROKER_PORT, 60)
    responder.loop_start()
    time.sleep(0.5)
    
    # Subscribe to request topic
    responder.subscribe(REQUEST_TOPIC, qos=1)
    time.sleep(0.5)
    
    # Send multiple concurrent requests
    NUM_REQUESTS = 5
    print(f"\n[Requester] Sending {NUM_REQUESTS} concurrent requests...")
    
    for i in range(NUM_REQUESTS):
        correlation_id = uuid.uuid4().bytes
        request_payload = {
            'sensor_id': f'sensor_{i+1:03d}',
            'action': 'read_temperature',
            'request_id': i + 1
        }
        
        request_props = Properties(PacketTypes.PUBLISH)
        request_props.ResponseTopic = response_topic
        request_props.CorrelationData = correlation_id
        
        request_time = time.time()
        result = requester.publish(
            REQUEST_TOPIC,
            json.dumps(request_payload),
            qos=1,
            properties=request_props
        )
        
        requests_sent.append({
            'correlation_data': correlation_id,
            'payload': request_payload,
            'timestamp': request_time
        })
        
        time.sleep(0.05)  # Small delay between requests
    
    # Wait for all responses
    print(f"[Requester] Waiting for {NUM_REQUESTS} responses...")
    time.sleep(2)
    
    # Verify all requests received by responder
    if len(service_requests_received) != NUM_REQUESTS:
        print(f"✗ TEST 2 FAILED: Expected {NUM_REQUESTS} requests, responder received {len(service_requests_received)}")
        requester.loop_stop()
        requester.disconnect()
        responder.loop_stop()
        responder.disconnect()
        return False
    
    # Verify all responses received by requester
    if len(responses_received) != NUM_REQUESTS:
        print(f"✗ TEST 2 FAILED: Expected {NUM_REQUESTS} responses, requester received {len(responses_received)}")
        requester.loop_stop()
        requester.disconnect()
        responder.loop_stop()
        responder.disconnect()
        return False
    
    # Verify all correlation IDs match
    sent_ids = set(req['correlation_data'] for req in requests_sent)
    received_ids = set(resp['correlation_data'] for resp in responses_received)
    
    if sent_ids != received_ids:
        print(f"✗ TEST 2 FAILED: Correlation IDs mismatch")
        print(f"  Sent: {len(sent_ids)} unique IDs")
        print(f"  Received: {len(received_ids)} unique IDs")
        success = False
    else:
        print(f"\n✓ TEST 2 PASSED: Concurrent request-response working")
        print(f"  Requests sent: {NUM_REQUESTS}")
        print(f"  Responses received: {len(responses_received)}")
        print(f"  All correlation IDs matched correctly")
        success = True
    
    # Cleanup
    requester.loop_stop()
    requester.disconnect()
    responder.loop_stop()
    responder.disconnect()
    time.sleep(0.5)
    
    return success

def run_test():
    """Run all request-response pattern tests"""
    print("\n" + "="*70)
    print("MQTT v5.0 REQUEST/RESPONSE PATTERN TEST")
    print("="*70)
    
    try:
        # Run all tests
        test1_pass = test_simple_request_response()
        test2_pass = test_concurrent_requests()
        
        # Summary
        print("\n" + "="*70)
        print("TEST SUMMARY")
        print("="*70)
        print(f"Test 1 (Simple Request-Response): {'✓ PASSED' if test1_pass else '✗ FAILED'}")
        print(f"Test 2 (Concurrent Requests): {'✓ PASSED' if test2_pass else '✗ FAILED'}")
        
        all_pass = test1_pass and test2_pass
        
        print("\n" + "="*70)
        if all_pass:
            print("✓✓✓ REQUEST/RESPONSE PATTERN TEST PASSED ✓✓✓")
            print("Response Topic and Correlation Data properties working correctly!")
        else:
            print("✗✗✗ REQUEST/RESPONSE PATTERN TEST FAILED ✗✗✗")
        print("="*70 + "\n")
        
        return all_pass
        
    except Exception as e:
        print(f"\n✗ TEST ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = run_test()
    sys.exit(0 if success else 1)
