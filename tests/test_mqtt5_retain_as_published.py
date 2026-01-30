#!/usr/bin/env python3
"""
Test MQTT v5 Retain as Published (RAP) subscription option
RAP controls whether the retain flag is preserved or cleared when forwarding messages.
"""

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion
import time
import pytest

pytestmark = pytest.mark.mqtt5


BROKER_HOST = "localhost"
BROKER_PORT = 1883

class TestRetainAsPublished:
    """Test MQTT v5 Retain as Published subscription option"""
    
    def test_rap_false_clears_retain_flag(self):
        """Test that RAP=false (default) clears the retain flag on forwarded messages"""
        topic = "test/rap/false"
        test_payload = b"retained message"
        
        # Clear any existing retained message
        clear_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        clear_client.connect(BROKER_HOST, BROKER_PORT)
        clear_client.publish(topic, payload=None, retain=True)
        time.sleep(0.5)
        clear_client.disconnect()
        
        # Publisher: Publish a retained message FIRST (before any subscriber)
        pub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_publisher", protocol=mqtt.MQTTv5)
        pub_client.connect(BROKER_HOST, BROKER_PORT)
        pub_client.loop_start()
        print(f"DEBUG: Publishing retained message")
        result = pub_client.publish(topic, test_payload, qos=1, retain=True)
        result.wait_for_publish()
        print(f"DEBUG: Publish complete, disconnecting publisher")
        pub_client.loop_stop()
        pub_client.disconnect()
        time.sleep(1.0)  # Ensure message is stored
        
        # NOW subscribe with RAP=false (default)
        received_messages = []
        sub_ready = False
        
        def on_message(client, userdata, msg):
            print(f"DEBUG: Received message on topic={msg.topic}")
            received_messages.append({
                'topic': msg.topic,
                'payload': msg.payload,
                'retain': msg.retain
            })
        
        def on_subscribe(client, userdata, mid, reason_code_list, properties):
            nonlocal sub_ready
            sub_ready = True
            print(f"DEBUG: Subscription completed with reason_codes={reason_code_list}")
        
        def on_connect(client, userdata, flags, reason_code, properties):
            print(f"DEBUG: Connected with result code {reason_code}")
            # Subscribe AFTER connection completes
            print(f"DEBUG: Subscribing to topic {topic}")
            result, mid = client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=False))
            print(f"DEBUG: Subscribe called, result={result}, mid={mid}")
        
        sub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_sub_false", protocol=mqtt.MQTTv5)
        sub_client.on_message = on_message
        sub_client.on_subscribe = on_subscribe
        sub_client.on_connect = on_connect
        sub_client.connect(BROKER_HOST, BROKER_PORT)
        sub_client.loop_start()
        
        # Wait for subscription to complete
        timeout = time.time() + 5
        while not sub_ready and time.time() < timeout:
            time.sleep(0.1)
        assert sub_ready, "Subscription did not complete"
        
        # Wait for the retained message to be delivered
        print(f"DEBUG: Waiting for retained message delivery...")
        time.sleep(2.0)
        print(f"DEBUG: Done waiting, received {len(received_messages)} messages")
        
        # Verify: Should receive the retained message with retain=False (cleared by RAP=false)
        print(f"DEBUG: Received messages: {received_messages}")
        assert len(received_messages) == 1, f"Expected 1 message (retained delivery), got {len(received_messages)}"
        assert received_messages[0]['payload'] == test_payload
        assert received_messages[0]['retain'] == False, f"RAP=false should clear retain flag on retained delivery, but got retain={received_messages[0]['retain']}"
        
        sub_client.loop_stop()
        sub_client.disconnect()
    
    def test_rap_true_preserves_retain_flag(self):
        """Test that RAP=true preserves the retain flag on forwarded messages"""
        topic = "test/rap/true"
        test_payload = b"retained message preserved"
        
        # Clear any existing retained message
        clear_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        clear_client.connect(BROKER_HOST, BROKER_PORT)
        clear_client.publish(topic, payload=None, retain=True)
        time.sleep(0.5)
        clear_client.disconnect()
        
        # Publisher: Publish a retained message FIRST (before any subscriber)
        pub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_publisher2", protocol=mqtt.MQTTv5)
        pub_client.connect(BROKER_HOST, BROKER_PORT)
        pub_client.loop_start()
        result = pub_client.publish(topic, test_payload, qos=1, retain=True)
        result.wait_for_publish()
        pub_client.loop_stop()
        pub_client.disconnect()
        time.sleep(1.0)  # Ensure message is stored
        
        # NOW subscribe with RAP=true
        received_messages = []
        sub_ready = False
        
        def on_message(client, userdata, msg):
            received_messages.append({
                'topic': msg.topic,
                'payload': msg.payload,
                'retain': msg.retain
            })
        
        def on_subscribe(client, userdata, mid, reason_code_list, properties):
            nonlocal sub_ready
            sub_ready = True
        
        def on_connect(client, userdata, flags, reason_code, properties):
            # Subscribe with RAP=true (bit 3 = 1)
            client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=True))
        
        sub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_sub_true", protocol=mqtt.MQTTv5)
        sub_client.on_message = on_message
        sub_client.on_subscribe = on_subscribe
        sub_client.on_connect = on_connect
        sub_client.connect(BROKER_HOST, BROKER_PORT)
        sub_client.loop_start()
        
        # Wait for subscription
        timeout = time.time() + 5
        while not sub_ready and time.time() < timeout:
            time.sleep(0.1)
        assert sub_ready, "Subscription did not complete"
        
        # Wait for the retained message to be delivered
        time.sleep(2.0)
        
        # Verify: Should receive the retained message with retain=True (preserved by RAP=true)
        assert len(received_messages) == 1, f"Expected 1 message (retained delivery), got {len(received_messages)}"
        assert received_messages[0]['payload'] == test_payload
        assert received_messages[0]['retain'] == True, "RAP=true should preserve retain flag on retained delivery"
        
        sub_client.loop_stop()
        sub_client.disconnect()
    
    def test_rap_non_retained_message(self):
        """Test that non-retained messages remain non-retained regardless of RAP setting"""
        topic = "test/rap/non_retained"
        test_payload = b"non-retained message"
        
        # Two subscribers: one with RAP=false, one with RAP=true
        received_rap_false = []
        received_rap_true = []
        sub1_ready = False
        sub2_ready = False
        
        def on_message_false(client, userdata, msg):
            received_rap_false.append({
                'retain': msg.retain,
                'payload': msg.payload
            })
        
        def on_message_true(client, userdata, msg):
            received_rap_true.append({
                'retain': msg.retain,
                'payload': msg.payload
            })
        
        def on_subscribe_false(client, userdata, mid, reason_code_list, properties):
            nonlocal sub1_ready
            sub1_ready = True
        
        def on_subscribe_true(client, userdata, mid, reason_code_list, properties):
            nonlocal sub2_ready
            sub2_ready = True
        
        def on_connect_false(client, userdata, flags, reason_code, properties):
            client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=False))
        
        def on_connect_true(client, userdata, flags, reason_code, properties):
            client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=True))
        
        # Subscriber 1: RAP=false
        sub_client_false = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_non_ret_false", protocol=mqtt.MQTTv5)
        sub_client_false.on_message = on_message_false
        sub_client_false.on_subscribe = on_subscribe_false
        sub_client_false.on_connect = on_connect_false
        sub_client_false.connect(BROKER_HOST, BROKER_PORT)
        sub_client_false.loop_start()
        
        # Subscriber 2: RAP=true
        sub_client_true = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_non_ret_true", protocol=mqtt.MQTTv5)
        sub_client_true.on_message = on_message_true
        sub_client_true.on_subscribe = on_subscribe_true
        sub_client_true.on_connect = on_connect_true
        sub_client_true.connect(BROKER_HOST, BROKER_PORT)
        sub_client_true.loop_start()
        
        # Wait for both subscriptions
        timeout = time.time() + 5
        while (not sub1_ready or not sub2_ready) and time.time() < timeout:
            time.sleep(0.1)
        assert sub1_ready and sub2_ready, "Subscriptions did not complete"
        
        # Publish a non-retained message
        pub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_non_ret_pub", protocol=mqtt.MQTTv5)
        pub_client.connect(BROKER_HOST, BROKER_PORT)
        pub_client.loop_start()
        result = pub_client.publish(topic, test_payload, qos=1, retain=False)
        result.wait_for_publish()
        time.sleep(1.0)
        pub_client.loop_stop()
        pub_client.disconnect()
        
        # Wait for messages to be received
        time.sleep(1.0)
        
        # Both should receive message with retain=False
        assert len(received_rap_false) == 1
        assert received_rap_false[0]['retain'] == False, "Non-retained message should stay non-retained (RAP=false)"
        
        assert len(received_rap_true) == 1
        assert received_rap_true[0]['retain'] == False, "Non-retained message should stay non-retained (RAP=true)"
        
        sub_client_false.loop_stop()
        sub_client_false.disconnect()
        sub_client_true.loop_stop()
        sub_client_true.disconnect()
    
    def test_rap_multiple_subscribers_different_settings(self):
        """Test multiple subscribers with different RAP settings on the same topic"""
        topic = "test/rap/multiple"
        test_payload = b"mixed RAP settings"
        
        # Publish retained message first
        pub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_multi_pub", protocol=mqtt.MQTTv5)
        pub_client.connect(BROKER_HOST, BROKER_PORT)
        pub_client.loop_start()
        result = pub_client.publish(topic, test_payload, qos=1, retain=True)
        result.wait_for_publish()
        pub_client.loop_stop()
        pub_client.disconnect()
        time.sleep(1.0)  # Ensure message is stored
        
        # Three subscribers with different RAP settings
        received_rap_false_1 = []
        received_rap_false_2 = []
        received_rap_true = []
        sub1_ready = False
        sub2_ready = False
        sub3_ready = False
        
        def on_message_false_1(client, userdata, msg):
            received_rap_false_1.append(msg.retain)
        
        def on_message_false_2(client, userdata, msg):
            received_rap_false_2.append(msg.retain)
        
        def on_message_true(client, userdata, msg):
            received_rap_true.append(msg.retain)
        
        def on_subscribe_1(client, userdata, mid, reason_code_list, properties):
            nonlocal sub1_ready
            sub1_ready = True
        
        def on_subscribe_2(client, userdata, mid, reason_code_list, properties):
            nonlocal sub2_ready
            sub2_ready = True
        
        def on_subscribe_3(client, userdata, mid, reason_code_list, properties):
            nonlocal sub3_ready
            sub3_ready = True
        
        def on_connect_1(client, userdata, flags, reason_code, properties):
            client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=False))
        
        def on_connect_2(client, userdata, flags, reason_code, properties):
            client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=False))
        
        def on_connect_3(client, userdata, flags, reason_code, properties):
            client.subscribe(topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=True))
        
        # Subscriber 1: RAP=false
        sub1 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_multi_sub1", protocol=mqtt.MQTTv5)
        sub1.on_message = on_message_false_1
        sub1.on_subscribe = on_subscribe_1
        sub1.on_connect = on_connect_1
        sub1.connect(BROKER_HOST, BROKER_PORT)
        sub1.loop_start()
        
        # Subscriber 2: RAP=false
        sub2 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_multi_sub2", protocol=mqtt.MQTTv5)
        sub2.on_message = on_message_false_2
        sub2.on_subscribe = on_subscribe_2
        sub2.on_connect = on_connect_2
        sub2.connect(BROKER_HOST, BROKER_PORT)
        sub2.loop_start()
        
        # Subscriber 3: RAP=true
        sub3 = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_multi_sub3", protocol=mqtt.MQTTv5)
        sub3.on_message = on_message_true
        sub3.on_subscribe = on_subscribe_3
        sub3.on_connect = on_connect_3
        sub3.connect(BROKER_HOST, BROKER_PORT)
        sub3.loop_start()
        
        # Wait for all subscriptions
        timeout = time.time() + 5
        while (not sub1_ready or not sub2_ready or not sub3_ready) and time.time() < timeout:
            time.sleep(0.1)
        assert sub1_ready and sub2_ready and sub3_ready, "Subscriptions did not complete"
        
        time.sleep(2.0)  # Wait for retained messages to be delivered
        
        # Verify each subscriber received correct retain flag based on their RAP setting
        assert len(received_rap_false_1) == 1
        assert received_rap_false_1[0] == False, "Subscriber 1 (RAP=false) should receive retain=False"
        
        assert len(received_rap_false_2) == 1
        assert received_rap_false_2[0] == False, "Subscriber 2 (RAP=false) should receive retain=False"
        
        assert len(received_rap_true) == 1
        assert received_rap_true[0] == True, "Subscriber 3 (RAP=true) should receive retain=True"
        
        sub1.loop_stop()
        sub1.disconnect()
        sub2.loop_stop()
        sub2.disconnect()
        sub3.loop_stop()
        sub3.disconnect()
    
    def test_rap_with_wildcard_subscriptions(self):
        """Test RAP with wildcard subscriptions"""
        topic = "test/rap/wildcard/sensor1"
        wildcard_topic = "test/rap/wildcard/+"
        test_payload = b"wildcard retained"
        
        # Publish retained message
        pub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_wc_pub", protocol=mqtt.MQTTv5)
        pub_client.connect(BROKER_HOST, BROKER_PORT)
        pub_client.loop_start()
        result = pub_client.publish(topic, test_payload, qos=1, retain=True)
        result.wait_for_publish()
        pub_client.loop_stop()
        pub_client.disconnect()
        time.sleep(1.0)  # Ensure message is stored
        
        # Subscriber with wildcard and RAP=true
        received_messages = []
        sub_ready = False
        
        def on_message(client, userdata, msg):
            received_messages.append({
                'topic': msg.topic,
                'retain': msg.retain
            })
        
        def on_subscribe(client, userdata, mid, reason_code_list, properties):
            nonlocal sub_ready
            sub_ready = True
        
        def on_connect(client, userdata, flags, reason_code, properties):
            client.subscribe(wildcard_topic, options=mqtt.SubscribeOptions(qos=1, retainAsPublished=True))
        
        sub_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, client_id="rap_wc_sub", protocol=mqtt.MQTTv5)
        sub_client.on_message = on_message
        sub_client.on_subscribe = on_subscribe
        sub_client.on_connect = on_connect
        sub_client.connect(BROKER_HOST, BROKER_PORT)
        sub_client.loop_start()
        
        # Wait for subscription
        timeout = time.time() + 5
        while not sub_ready and time.time() < timeout:
            time.sleep(0.1)
        assert sub_ready, "Subscription did not complete"
        
        time.sleep(2.0)  # Wait for retained message to be delivered
        
        # Should receive retained message with retain=True
        assert len(received_messages) == 1
        assert received_messages[0]['topic'] == topic
        assert received_messages[0]['retain'] == True, "Wildcard subscription with RAP=true should preserve retain flag"
        
        sub_client.loop_stop()
        sub_client.disconnect()

if __name__ == "__main__":
    print("Testing MQTT v5 Retain as Published (RAP) subscription option...")
    test_instance = TestRetainAsPublished()
    
    try:
        print("\n1. Testing RAP=false clears retain flag...")
        test_instance.test_rap_false_clears_retain_flag()
        print("   ✓ PASSED")
        
        print("\n2. Testing RAP=true preserves retain flag...")
        test_instance.test_rap_true_preserves_retain_flag()
        print("   ✓ PASSED")
        
        print("\n3. Testing non-retained messages...")
        test_instance.test_rap_non_retained_message()
        print("   ✓ PASSED")
        
        print("\n4. Testing multiple subscribers with different RAP settings...")
        test_instance.test_rap_multiple_subscribers_different_settings()
        print("   ✓ PASSED")
        
        print("\n5. Testing RAP with wildcard subscriptions...")
        test_instance.test_rap_with_wildcard_subscriptions()
        print("   ✓ PASSED")
        
        print("\n✅ All RAP tests passed!")
        
    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        raise
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        raise
