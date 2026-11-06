import socket
import struct
import time
import os
import pytest
import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion


# Kafka API Keys
API_FETCH = 1
API_METADATA = 3
API_OFFSET_COMMIT = 8
API_OFFSET_FETCH = 9
API_FIND_COORDINATOR = 10
API_API_VERSIONS = 18
API_SASL_HANDSHAKE = 17
API_SASL_AUTHENTICATE = 36

def authenticate_socket(sock, username, password, mode="standard"):
    # Step A: SaslHandshake (API Key 17)
    handshake_payload = pack_string("PLAIN")
    # Handshake v0 means raw PLAIN tokens, v1 means SaslAuthenticate requests
    handshake_version = 1 if mode == "standard" else 0
    request = make_request(API_SASL_HANDSHAKE, handshake_version, 9999, "auth-helper", handshake_payload)
    sock.sendall(request)
    read_response(sock)

    # Step B: Authentication
    token = b"\x00" + username.encode('utf-8') + b"\x00" + password.encode('utf-8')
    if mode == "standard":
        auth_payload = struct.pack('>i', len(token)) + token
        auth_request = make_request(API_SASL_AUTHENTICATE, 0, 10000, "auth-helper", auth_payload)
        sock.sendall(auth_request)
        read_response(sock)
    else:
        raw_payload = struct.pack('>i', len(token)) + token
        sock.sendall(raw_payload)
        sock.recv(4)

def pack_string(s):
    if s is None:
        return struct.pack('>h', -1)
    encoded = s.encode('utf-8')
    return struct.pack('>h', len(encoded)) + encoded

def unpack_string(data, offset):
    length, = struct.unpack_from('>h', data, offset)
    offset += 2
    if length == -1:
        return None, offset
    s = data[offset:offset+length].decode('utf-8')
    offset += length
    return s, offset

def make_request(api_key, api_version, correlation_id, client_id, payload):
    header = struct.pack('>hhi', api_key, api_version, correlation_id)
    header += pack_string(client_id)
    body = header + payload
    length = len(body)
    return struct.pack('>i', length) + body

def read_response(sock):
    length_bytes = sock.recv(4)
    if not length_bytes:
        return None
    length, = struct.unpack('>i', length_bytes)
    data = b''
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            break
        data += chunk
    return data

@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_api_versions(broker_config):
    """Test standard ApiVersions request over TCP socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")

    try:
        # Build ApiVersions request (Version 0)
        request = make_request(API_API_VERSIONS, 0, 101, "test-client", b"")
        sock.sendall(request)

        response = read_response(sock)
        assert response is not None
        
        correlation_id, error_code = struct.unpack_from('>ih', response, 0)
        assert correlation_id == 101
        assert error_code == 0 # Success
        
        # Read API keys array length
        api_keys_len, = struct.unpack_from('>i', response, 6)
        assert api_keys_len >= 5
    finally:
        sock.close()

@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_metadata(broker_config):
    """Test standard Metadata request over TCP socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")

    try:
        if broker_config["username"]:
            authenticate_socket(sock, "Admin", "Admin")
        # Build Metadata request (Version 0)
        # Topics array length (0 = all topics)
        payload = struct.pack('>i', 0)
        request = make_request(API_METADATA, 0, 102, "test-client", payload)
        sock.sendall(request)

        response = read_response(sock)
        assert response is not None
        
        correlation_id, = struct.unpack_from('>i', response, 0)
        assert correlation_id == 102
        
        # Read brokers array length
        brokers_len, = struct.unpack_from('>i', response, 4)
        assert brokers_len == 1
        
        # Unpack broker 0: node_id (INT32), host (String), port (INT32)
        node_id, = struct.unpack_from('>i', response, 8)
        assert node_id == 0
        host, offset = unpack_string(response, 12)
        port, = struct.unpack_from('>i', response, offset)
        assert port == 9092
    finally:
        sock.close()

@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_mqtt_to_kafka_fetching(broker_config):
    """Publish MQTT message and verify it can be fetched via Kafka wire protocol."""
    mqtt_client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    if broker_config["username"]:
        mqtt_client.username_pw_set(broker_config["username"], broker_config["password"])

    connected = False
    def on_connect(client, userdata, flags, reason_code, properties=None):
        nonlocal connected
        connected = (reason_code == 0)
    mqtt_client.on_connect = on_connect
    
    mqtt_client.connect(broker_config["host"], broker_config["port"])
    mqtt_client.loop_start()
    
    # Wait for MQTT connection
    start = time.time()
    while not connected and (time.time() - start) < 5.0:
        time.sleep(0.05)
    assert connected, "MQTT client failed to connect to broker"
    
    test_payload = b"{\"value\": 42.0}"
    pub_info = mqtt_client.publish("sensors/temp", test_payload, qos=1)
    pub_info.wait_for_publish(timeout=5.0)
    time.sleep(0.5) # Wait for batch flush to DB
    
    # 2. Connect via Kafka socket and send Fetch request
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")

    try:
        if broker_config["username"]:
            authenticate_socket(sock, "Admin", "Admin")
        # Build Fetch request (Version 0):
        # replica_id (i), max_wait_ms (i), min_bytes (i), topic_count (i)
        fetch_header = struct.pack('>iiii', -1, 500, 1, 1)
        
        # Topic name string + partitions array count (1)
        topic_payload = pack_string("sensors/temp") + struct.pack('>i', 1)
        
        # Partition 0: partition_id (i), fetch_offset (q, INT64), max_bytes (i)
        # Fetching starting from offset 0
        partition_payload = struct.pack('>iqi', 0, 0, 1024 * 1024)
        
        request = make_request(API_FETCH, 0, 103, "test-client", fetch_header + topic_payload + partition_payload)
        sock.sendall(request)

        response = read_response(sock)
        assert response is not None
        
        correlation_id, = struct.unpack_from('>i', response, 0)
        assert correlation_id == 103
        
        # Topics array count
        topics_count, = struct.unpack_from('>i', response, 4)
        assert topics_count == 1
        
        topic_name, offset = unpack_string(response, 8)
        assert topic_name == "sensors/temp"
        
        # Partition count
        partitions_count, = struct.unpack_from('>i', response, offset)
        assert partitions_count == 1
        
        # Unpack partition response: partition_id (i), error_code (h), high_watermark (q), record_set_size (i)
        partition_id, error_code, high_watermark, record_set_size = struct.unpack_from('>ihqi', response, offset + 4)
        assert partition_id == 0
        assert error_code == 0
        assert high_watermark >= 1
        assert record_set_size > 0
        
        # MessageSet record offset
        record_offset = offset + 4 + 4 + 2 + 8 + 4
        
        # Standard MessageSet V0 layout:
        # offset (q, INT64), msg_size (i, INT32), crc (i), magic (b), attributes (b), key_length (i, INT32), key, value_length (i, INT32), value (bytes)
        msg_offset, msg_size = struct.unpack_from('>qi', response, record_offset)
        assert msg_size > 0
        
        crc, magic, attr = struct.unpack_from('>ibb', response, record_offset + 12)
        assert magic == 0
        assert attr == 0
        
        key_len, = struct.unpack_from('>i', response, record_offset + 18)
        assert key_len == len("sensors/temp")
        
        key_data = response[record_offset + 22 : record_offset + 22 + key_len]
        assert key_data == b"sensors/temp"
        
        val_offset = record_offset + 22 + key_len
        val_len, = struct.unpack_from('>i', response, val_offset)
        assert val_len == len(test_payload)
        
        value_data = response[val_offset + 4 : val_offset + 4 + val_len]
        assert value_data == test_payload

        
    finally:
        sock.close()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_server_device_lifecycle(broker_config):
    """Test dynamic database-driven Kafka Server device lifecycle over GraphQL."""
    import requests
    graphql_url = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
    
    # 1. Quick check if GraphQL is available and get auth token if UserManagement is enabled
    headers = {}
    try:
        if broker_config["username"]:
            login_query = """
                mutation($username: String!, $password: String!) {
                    login(username: $username, password: $password) { token }
                }
            """
            resp = requests.post(graphql_url, json={"query": login_query, "variables": {"username": broker_config["username"], "password": broker_config["password"]}}, timeout=5)
            if resp.status_code == 200:
                body = resp.json()
                if "data" in body and body["data"] and "login" in body["data"] and body["data"]["login"]:
                    token = body["data"]["login"]["token"]
                    headers = {"Authorization": f"Bearer {token}"}
    except Exception:
        pass

    try:
        requests.post(graphql_url, json={"query": "{ __typename }"}, headers=headers, timeout=2)
    except Exception as e:
        pytest.skip(f"GraphQL endpoint not reachable at {graphql_url}: {e}")

    server_name = "pytest-kafka-server"
    test_port = 9093

    # Clean up any leftover device first
    cleanup_mutation = """
    mutation Delete($name: String!) {
      kafkaServer {
        delete(name: $name)
      }
    }
    """
    try:
        requests.post(graphql_url, json={"query": cleanup_mutation, "variables": {"name": server_name}}, headers=headers, timeout=5)
    except Exception:
        pass

    # 2. Add a new Kafka Server device via GraphQL mutation
    add_mutation = """
    mutation Add($input: KafkaServerInput!) {
      kafkaServer {
        add(input: $input) {
          success
          errors
          server {
            name
            port
            enabled
            status
          }
        }
      }
    }
    """
    variables = {
        "input": {
            "name": server_name,
            "namespace": "default",
            "nodeId": "*",
            "enabled": True,
            "host": "0.0.0.0",
            "port": test_port,
            "streams": [
                {
                    "streamName": "pytest_temp",
                    "topicFilter": "pytest/temp",
                    "retentionHours": 72
                }
            ]
        }
    }

    resp = requests.post(graphql_url, json={"query": add_mutation, "variables": variables}, headers=headers, timeout=5)
    resp.raise_for_status()
    result = resp.json()
    assert "errors" not in result, f"GraphQL response has errors: {result}"
    
    add_data = result["data"]["kafkaServer"]["add"]
    assert add_data["success"], f"Mutation failed: {add_data['errors']}"
    assert add_data["server"]["name"] == server_name
    assert add_data["server"]["port"] == test_port
    assert add_data["server"]["enabled"] is True

    # Allow Vert.x a moment to process EventBus config change and start TCP socket
    time.sleep(1.0)

    # 3. Query list of servers and verify existence and status
    list_query = """
    query GetServers {
      kafkaServers {
        name
        port
        enabled
        status
      }
    }
    """
    resp = requests.post(graphql_url, json={"query": list_query}, headers=headers, timeout=5)
    resp.raise_for_status()
    result = resp.json()
    assert "errors" not in result
    
    servers = result["data"]["kafkaServers"]
    pytest_server = next((s for s in servers if s["name"] == server_name), None)
    assert pytest_server is not None
    assert pytest_server["port"] == test_port
    assert pytest_server["status"] == "RUNNING"

    # 4. Assert that socket port 9093 is active and accepting connections
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2.0)
    try:
        sock.connect((broker_config["host"], test_port))
        # Send ApiVersions request (Version 0) to verify Kafka protocol compatibility
        request = make_request(API_API_VERSIONS, 0, 999, "pytest-lifecycle-client", b"")
        sock.sendall(request)
        response = read_response(sock)
        assert response is not None
        correlation_id, error_code = struct.unpack_from('>ih', response, 0)
        assert correlation_id == 999
        assert error_code == 0
    except Exception as e:
        pytest.fail(f"Failed to connect to dynamic Kafka Server on port {test_port}: {e}")
    finally:
        sock.close()

    # 5. Toggle the server to disabled
    toggle_mutation = """
    mutation Toggle($name: String!, $enabled: Boolean!) {
      kafkaServer {
        toggle(name: $name, enabled: $enabled) {
          success
          errors
          server {
            name
            enabled
            status
          }
        }
      }
    }
    """
    resp = requests.post(graphql_url, json={
        "query": toggle_mutation,
        "variables": {"name": server_name, "enabled": False}
    }, headers=headers, timeout=5)
    resp.raise_for_status()
    result = resp.json()
    assert "errors" not in result
    
    toggle_data = result["data"]["kafkaServer"]["toggle"]
    assert toggle_data["success"]
    assert toggle_data["server"]["enabled"] is False
    assert toggle_data["server"]["status"] == "STOPPED"

    # Allow Vert.x a moment to gracefully stop and close TCP port
    time.sleep(1.0)

    # 6. Verify that port 9093 is closed and no longer accepting connections
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1.0)
    try:
        sock.connect((broker_config["host"], test_port))
        pytest.fail(f"Port {test_port} is still open after dynamic disabling!")
    except socket.error:
        # Success, connection should be refused
        pass
    finally:
        sock.close()

    # 7. Delete the server device
    delete_mutation = """
    mutation Delete($name: String!) {
      kafkaServer {
        delete(name: $name)
      }
    }
    """
    resp = requests.post(graphql_url, json={
        "query": delete_mutation,
        "variables": {"name": server_name}
    }, headers=headers, timeout=5)
    resp.raise_for_status()
    result = resp.json()
    assert "errors" not in result
    assert result["data"]["kafkaServer"]["delete"] is True


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_write_authorization_validation(broker_config):
    """Test Kafka stream write authorization and topic filter validation."""
    import requests
    from kafka import KafkaProducer
    import json
    graphql_url = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
    
    # 1. Quick check if GraphQL is available and get auth token if UserManagement is enabled
    headers = {}
    try:
        if broker_config["username"]:
            login_query = """
                mutation($username: String!, $password: String!) {
                    login(username: $username, password: $password) { token }
                }
            """
            resp = requests.post(graphql_url, json={"query": login_query, "variables": {"username": broker_config["username"], "password": broker_config["password"]}}, timeout=5)
            if resp.status_code == 200:
                body = resp.json()
                if "data" in body and body["data"] and "login" in body["data"] and body["data"]["login"]:
                    token = body["data"]["login"]["token"]
                    headers = {"Authorization": f"Bearer {token}"}
    except Exception:
        pass

    try:
        requests.post(graphql_url, json={"query": "{ __typename }"}, headers=headers, timeout=2)
    except Exception as e:
        pytest.skip(f"GraphQL endpoint not reachable at {graphql_url}: {e}")

    server_name = "auth-test-server"
    test_port = 9094

    # Clean up any leftover device first
    cleanup_mutation = """
    mutation Delete($name: String!) {
      kafkaServer { delete(name: $name) }
    }
    """
    try:
        requests.post(graphql_url, json={"query": cleanup_mutation, "variables": {"name": server_name}}, headers=headers, timeout=5)
    except Exception:
        pass

    # 2. Add a new Kafka Server device with allowWrite constraints via GraphQL mutation
    add_mutation = """
    mutation Add($input: KafkaServerInput!) {
      kafkaServer {
        add(input: $input) {
          success
          errors
        }
      }
    }
    """
    variables = {
        "input": {
            "name": server_name,
            "namespace": "default",
            "nodeId": "*",
            "enabled": True,
            "host": "0.0.0.0",
            "port": test_port,
            "streams": [
                {
                    "streamName": "test_auth_stream",
                    "topicFilter": "pytest/auth/allowed/+",
                    "retentionHours": 72,
                    "allowWrite": True
                },
                {
                    "streamName": "test_auth_stream",
                    "topicFilter": "pytest/auth/forbidden/+",
                    "retentionHours": 72,
                    "allowWrite": False
                }
            ]
        }
    }

    resp = requests.post(graphql_url, json={"query": add_mutation, "variables": variables}, headers=headers, timeout=5)
    resp.raise_for_status()
    result = resp.json()
    assert "errors" not in result, f"GraphQL response has errors: {result}"
    assert result["data"]["kafkaServer"]["add"]["success"], f"Mutation failed: {result['data']['kafkaServer']['add']['errors']}"

    # Allow Vert.x a moment to process EventBus config change and start TCP socket
    time.sleep(1.5)

    producer = None
    try:
        # Initialize producer
        producer_kwargs = {
            "bootstrap_servers": [f"{broker_config['host']}:{test_port}"],
            "api_version": (2, 0, 0),
            "key_serializer": lambda k: str(k).encode("utf-8"),
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "retries": 0,
            "request_timeout_ms": 3000
        }
        if broker_config["username"]:
            producer_kwargs.update({
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_plain_username": "Admin",
                "sasl_plain_password": "Admin"
            })
        producer = KafkaProducer(**producer_kwargs)

        # 3. Publish to matching allowed topic filter -> should succeed!
        future = producer.send("test_auth_stream", key="pytest/auth/allowed/1", value={"data": "allowed"})
        record_metadata = future.get(timeout=3)
        assert record_metadata is not None

        # 4. Publish to matching forbidden topic filter -> should fail!
        with pytest.raises(Exception) as exc_info:
            future = producer.send("test_auth_stream", key="pytest/auth/forbidden/1", value={"data": "forbidden"})
            future.get(timeout=3)
        assert "TopicAuthorizationFailedException" in str(exc_info.value) or "Broker: Topic authorization failed" in str(exc_info.value) or "TopicAuthorizationFailedError" in str(exc_info.value)

        # 5. Publish to non-matching topic filter -> should fail!
        with pytest.raises(Exception) as exc_info:
            future = producer.send("test_auth_stream", key="pytest/auth/completely_invalid/1", value={"data": "invalid"})
            future.get(timeout=3)
        assert "TopicAuthorizationFailedException" in str(exc_info.value) or "Broker: Topic authorization failed" in str(exc_info.value) or "TopicAuthorizationFailedError" in str(exc_info.value)

        # 6. Publish to completely unconfigured stream/topic -> should succeed (Otherwise it can publish to any topic)
        future = producer.send("some_other_unconfigured_topic", key="any/topic/is/allowed/here", value={"data": "unconfigured"})
        record_metadata = future.get(timeout=3)
        assert record_metadata is not None

    finally:
        if producer:
            producer.close()
        # Clean up the test device
        requests.post(graphql_url, json={"query": cleanup_mutation, "variables": {"name": server_name}}, headers=headers, timeout=5)


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_sasl_authentication(broker_config):
    """Test SASL PLAIN authentication over Kafka protocol server."""
    # 1. Test standard SaslAuthenticate request (API Key 36)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")

    try:
        # Step A: SaslHandshake (API Key 17)
        handshake_payload = pack_string("PLAIN")
        request = make_request(17, 1, 1001, "test-auth-client", handshake_payload)
        sock.sendall(request)

        response = read_response(sock)
        assert response is not None
        correlation_id, error_code = struct.unpack_from('>ih', response, 0)
        assert correlation_id == 1001
        assert error_code == 0  # Success

        # Mechanisms array
        mechs_len, = struct.unpack_from('>i', response, 6)
        assert mechs_len >= 1
        mech_name, _ = unpack_string(response, 10)
        assert mech_name == "PLAIN"

        # Step B: SaslAuthenticate (API Key 36) with valid credentials
        username = "Admin"
        password = "Admin"
        token = b"\x00" + username.encode('utf-8') + b"\x00" + password.encode('utf-8')
        
        auth_payload = struct.pack('>i', len(token)) + token
        auth_request = make_request(36, 0, 1002, "test-auth-client", auth_payload)
        sock.sendall(auth_request)

        auth_response = read_response(sock)
        assert auth_response is not None
        correlation_id, error_code = struct.unpack_from('>ih', auth_response, 0)
        assert correlation_id == 1002
        assert error_code == 0  # Success (0)
        
    finally:
        sock.close()

    # 2. Test SaslAuthenticate with invalid credentials
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker_config["host"], 9092))
    try:
        # Step A: SaslHandshake
        handshake_payload = pack_string("PLAIN")
        request = make_request(17, 1, 1003, "test-auth-client", handshake_payload)
        sock.sendall(request)
        read_response(sock)

        # Step B: SaslAuthenticate with invalid credentials
        token = b"\x00" + b"invalid-user" + b"\x00" + b"invalid-pass"
        auth_payload = struct.pack('>i', len(token)) + token
        auth_request = make_request(36, 0, 1004, "test-auth-client", auth_payload)
        sock.sendall(auth_request)

        auth_response = read_response(sock)
        assert auth_response is not None
        correlation_id, error_code = struct.unpack_from('>ih', auth_response, 0)
        assert correlation_id == 1004
        # Allow 0 (Success) if user-management is disabled, or 58 if enabled
        assert error_code in (0, 58)
        
        # Verify connection is subsequently closed by server if auth failed
        if error_code == 58:
            sock.settimeout(2.0)
            try:
                data = sock.recv(1024)
                assert len(data) == 0  # Socket EOF
            except socket.timeout:
                pytest.fail("Connection was not closed by server after SASL authentication failure")
    finally:
        sock.close()

    # 3. Test raw socket PLAIN token fallback (SASL handshake V0 style)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((broker_config["host"], 9092))
    try:
        # Step A: SaslHandshake
        handshake_payload = pack_string("PLAIN")
        request = make_request(17, 0, 1005, "test-auth-client", handshake_payload)
        sock.sendall(request)
        read_response(sock)

        # Step B: Send raw token directly
        username = "Admin"
        password = "Admin"
        token = b"\x00" + username.encode('utf-8') + b"\x00" + password.encode('utf-8')
        raw_payload = struct.pack('>i', len(token)) + token
        sock.sendall(raw_payload)

        # Server responds with 4-byte length prefix of 0 (success)
        response_bytes = sock.recv(4)
        assert len(response_bytes) == 4
        length, = struct.unpack('>i', response_bytes)
        assert length == 0  # Success
    finally:
        sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_describe_configs(broker_config):
    """Test standard DescribeConfigs request over TCP socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")

    try:
        if broker_config["username"]:
            authenticate_socket(sock, "Admin", "Admin")

        # DescribeConfigsRequest Version 0:
        # resources_count (i), resource_type (b), resource_name (String), config_names_count (i)
        payload = struct.pack('>ib', 1, 2) # 1 resource, type 2 (Topic)
        payload += pack_string("sensors/temp")
        payload += struct.pack('>i', 0) # 0 config keys (all keys)

        request = make_request(32, 0, 105, "test-client", payload)
        sock.sendall(request)

        response = read_response(sock)
        assert response is not None

        correlation_id, = struct.unpack_from('>i', response, 0)
        assert correlation_id == 105

        # Response V0 fields:
        # throttle_time_ms (i), resources_count (i)
        throttle_time_ms, resources_count = struct.unpack_from('>ii', response, 4)
        assert throttle_time_ms == 0
        assert resources_count == 1

        # Resource 0: error_code (h), error_message (String), resource_type (b), resource_name (String), configs_count (i)
        error_code, = struct.unpack_from('>h', response, 12)
        assert error_code == 0
        
        # Unpack error_message
        err_msg, offset = unpack_string(response, 14)
        assert err_msg is None

        # Resource type
        res_type = response[offset]
        assert res_type == 2
        offset += 1

        # Resource name
        res_name, offset = unpack_string(response, offset)
        assert res_name == "sensors/temp"

        # Configs count
        configs_count, = struct.unpack_from('>i', response, offset)
        assert configs_count == 3
        offset += 4

        # Read configs...
        configs = {}
        for _ in range(configs_count):
            name, offset = unpack_string(response, offset)
            val, offset = unpack_string(response, offset)
            read_only, is_default, is_sensitive = struct.unpack_from('>bbb', response, offset)
            offset += 3
            configs[name] = val

        assert "cleanup.policy" in configs
        assert configs["cleanup.policy"] == "delete"
        assert "retention.ms" in configs

    finally:
        sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_metadata_v4(broker_config):
    """Test Metadata request Version 4 over TCP socket."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")

    try:
        if broker_config["username"]:
            authenticate_socket(sock, "Admin", "Admin")

        # MetadataRequest Version 4:
        # topics array count (i = 1), topic name (String), allow_auto_topic_creation (b = 1)
        payload = struct.pack('>i', 1)
        payload += pack_string("sensors/temp")
        payload += struct.pack('>b', 1) # allow_auto_topic_creation: True

        request = make_request(3, 4, 106, "test-client", payload)
        sock.sendall(request)

        response = read_response(sock)
        assert response is not None

        correlation_id, = struct.unpack_from('>i', response, 0)
        assert correlation_id == 106

        # Response V4 fields:
        # throttle_time_ms (i), brokers_count (i)
        throttle_time_ms, brokers_count = struct.unpack_from('>ii', response, 4)
        assert throttle_time_ms == 0
        assert brokers_count == 1

        # Unpack broker 0: node_id (i), host (String), port (i), rack (String)
        node_id, = struct.unpack_from('>i', response, 12)
        assert node_id == 0
        host, offset = unpack_string(response, 16)
        port, = struct.unpack_from('>i', response, offset)
        assert port == 9092
        offset += 4
        rack, offset = unpack_string(response, offset)
        assert rack is None

        # cluster_id (String)
        cluster_id, offset = unpack_string(response, offset)
        assert cluster_id == "monstermq-cluster"

        # controller_id (i)
        controller_id, = struct.unpack_from('>i', response, offset)
        assert controller_id == 0
        offset += 4

        # topics array count (i)
        topics_count, = struct.unpack_from('>i', response, offset)
        assert topics_count == 1
        offset += 4

        # Topic 0: error_code (h), name (String), is_internal (b), partitions_count (i)
        error_code, = struct.unpack_from('>h', response, offset)
        assert error_code == 0
        offset += 2

        topic_name, offset = unpack_string(response, offset)
        assert topic_name == "sensors/temp"

        is_internal = response[offset]
        assert is_internal == 0
        offset += 1

        partitions_count, = struct.unpack_from('>i', response, offset)
        assert partitions_count == 1
        offset += 4

        # Partition 0: error_code (h), partition_id (i), leader (i), replicas_count (i), replica (i), isr_count (i), isr (i)
        p_error, p_id, leader, replicas_count = struct.unpack_from('>hiii', response, offset)
        assert p_error == 0
        assert p_id == 0
        assert leader == 0
        assert replicas_count == 1
        offset += 2 + 4 + 4 + 4

        replica_id, isr_count = struct.unpack_from('>ii', response, offset)
        assert replica_id == 0
        assert isr_count == 1
        offset += 4 + 4

        isr_id, = struct.unpack_from('>i', response, offset)
        assert isr_id == 0

    finally:
        sock.close()




# ---------------------------------------------------------------------------
# Protocol behaviour shared with the edge broker (issue #237)
# ---------------------------------------------------------------------------
import contextlib
import threading
import zlib

API_PRODUCE = 0
API_LIST_OFFSETS = 2

STREAM_SERVER_NAME = "pytest-kafka-streams"
STREAM_SERVER_PORT = 9094
STORE_STREAM = "pytest_store"      # stream name differs from its MQTT filter on purpose
OTHER_STREAM = "pytest_other"


def _graphql_headers(broker_config):
    import requests
    graphql_url = os.getenv("GRAPHQL_URL", "http://localhost:4000/graphql")
    headers = {}
    try:
        if broker_config["username"]:
            login_query = "mutation($username: String!, $password: String!) { login(username: $username, password: $password) { token } }"
            resp = requests.post(graphql_url, json={"query": login_query, "variables": {"username": broker_config["username"], "password": broker_config["password"]}}, timeout=5)
            body = resp.json()
            if body.get("data") and body["data"].get("login"):
                headers = {"Authorization": f"Bearer {body['data']['login']['token']}"}
        requests.post(graphql_url, json={"query": "{ __typename }"}, headers=headers, timeout=2)
    except Exception as e:
        pytest.skip(f"GraphQL endpoint not reachable at {graphql_url}: {e}")
    return graphql_url, headers


@contextlib.contextmanager
def kafka_stream_server(broker_config):
    """Temporary Kafka server with two streams whose names differ from their MQTT filters."""
    import requests
    graphql_url, headers = _graphql_headers(broker_config)
    delete = "mutation($name: String!) { kafkaServer { delete(name: $name) } }"
    requests.post(graphql_url, json={"query": delete, "variables": {"name": STREAM_SERVER_NAME}}, headers=headers, timeout=5)

    add = """
    mutation Add($input: KafkaServerInput!) {
      kafkaServer { add(input: $input) { success errors } }
    }"""
    variables = {"input": {
        "name": STREAM_SERVER_NAME, "namespace": "default", "nodeId": "*", "enabled": True,
        "host": "0.0.0.0", "port": STREAM_SERVER_PORT,
        "advertisedHost": "kafka.example.test", "advertisedPort": 19094,
        "storeType": "SQLITE",
        "streams": [
            {"streamName": STORE_STREAM, "topicFilter": "pytest/store/#", "retentionHours": 1, "allowWrite": True},
            {"streamName": OTHER_STREAM, "topicFilter": "pytest/other/#", "retentionHours": 24, "allowWrite": True},
        ],
    }}
    resp = requests.post(graphql_url, json={"query": add, "variables": variables}, headers=headers, timeout=5)
    resp.raise_for_status()
    result = resp.json()
    assert "errors" not in result, result
    assert result["data"]["kafkaServer"]["add"]["success"], result
    time.sleep(1.5)
    try:
        yield graphql_url, headers
    finally:
        requests.post(graphql_url, json={"query": delete, "variables": {"name": STREAM_SERVER_NAME}}, headers=headers, timeout=5)


def connect_stream_server(broker_config):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10.0)
    sock.connect((broker_config["host"], STREAM_SERVER_PORT))
    if broker_config["username"]:
        authenticate_socket(sock, "Admin", "Admin")
    return sock


def legacy_message(key, value, attributes=0):
    body = struct.pack('>bb', 0, attributes)
    body += struct.pack('>i', len(key)) + key + struct.pack('>i', len(value)) + value
    msg = struct.pack('>i', zlib.crc32(body) & 0xffffffff) + body
    return struct.pack('>qi', 0, len(msg)) + msg


def produce_request(corr, topic, partition, message_set, acks=1):
    payload = struct.pack('>hii', acks, 5000, 1) + pack_string(topic)
    payload += struct.pack('>ii', 1, partition) + struct.pack('>i', len(message_set)) + message_set
    return make_request(API_PRODUCE, 0, corr, "pytest", payload)


def produce_error(sock, corr, topic, partition, key, value, attributes=0):
    sock.sendall(produce_request(corr, topic, partition, legacy_message(key, value, attributes)))
    resp = read_response(sock)
    assert struct.unpack_from('>i', resp, 0)[0] == corr
    offset = 8  # correlation + topic count
    _, offset = unpack_string(resp, offset)
    offset += 4  # partition count
    _, err = struct.unpack_from('>ih', resp, offset)
    return err


def fetch_request(corr, topic, partitions, fetch_offset=0, max_wait=0):
    payload = struct.pack('>iiii', -1, max_wait, 1, 1) + pack_string(topic) + struct.pack('>i', len(partitions))
    for p in partitions:
        payload += struct.pack('>iqi', p, fetch_offset, 1024 * 1024)
    return make_request(API_FETCH, 0, corr, "pytest", payload)


def parse_fetch(resp):
    """-> {topic: [(partition, error, high_watermark, [(offset, key, value)])]}"""
    topics = {}
    count, = struct.unpack_from('>i', resp, 4)
    offset = 8
    for _ in range(count):
        topic, offset = unpack_string(resp, offset)
        pcount, = struct.unpack_from('>i', resp, offset)
        offset += 4
        parts = []
        for _ in range(pcount):
            pid, err, hw, size = struct.unpack_from('>ihqi', resp, offset)
            offset += 4 + 2 + 8 + 4
            data = resp[offset:offset + size]
            offset += size
            msgs, pos = [], 0
            while pos < len(data):
                moff, msize = struct.unpack_from('>qi', data, pos)
                pos += 12
                klen, = struct.unpack_from('>i', data, pos + 6)
                key = data[pos + 10:pos + 10 + klen]
                vpos = pos + 10 + klen
                vlen, = struct.unpack_from('>i', data, vpos)
                msgs.append((moff, key, data[vpos + 4:vpos + 4 + vlen]))
                pos += msize
            parts.append((pid, err, hw, msgs))
        topics.setdefault(topic, []).extend(parts)
    return topics


def fetch(sock, corr, topic, partitions=(0,), fetch_offset=0, max_wait=0):
    sock.sendall(fetch_request(corr, topic, list(partitions), fetch_offset, max_wait))
    return parse_fetch(read_response(sock))


def latest_offset(sock, corr, topic, partition=0):
    payload = struct.pack('>ii', -1, 1) + pack_string(topic) + struct.pack('>iiqi', 1, partition, -1, 1)
    sock.sendall(make_request(API_LIST_OFFSETS, 0, corr, "pytest", payload))
    resp = read_response(sock)
    offset = 8
    _, offset = unpack_string(resp, offset)
    offset += 4
    pid, err, noffsets = struct.unpack_from('>ihi', resp, offset)
    value = struct.unpack_from('>q', resp, offset + 10)[0] if noffsets else None
    return err, value


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_api_versions_above_v0_falls_back(broker_config):
    """ApiVersions v3 answers UNSUPPORTED_VERSION (35) plus the v0-shaped key list (KIP-511)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")
    try:
        sock.sendall(make_request(API_API_VERSIONS, 3, 301, "pytest", b""))
        resp = read_response(sock)
        corr, err, count = struct.unpack_from('>ihi', resp, 0)
        assert corr == 301
        assert err == 35
        assert count >= 5
        assert len(resp) == 10 + count * 6  # v0 shape: no throttle time
    finally:
        sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_sasl_unsupported_mechanism(broker_config):
    """An unsupported SASL mechanism returns error 33 (UNSUPPORTED_SASL_MECHANISM)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")
    try:
        sock.sendall(make_request(API_SASL_HANDSHAKE, 1, 302, "pytest", pack_string("SCRAM-SHA-256")))
        resp = read_response(sock)
        corr, err = struct.unpack_from('>ih', resp, 0)
        assert corr == 302
        assert err == 33
    finally:
        sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_sasl_authenticate_v1_layout(broker_config):
    """SaslAuthenticate v1 responses carry session_lifetime_ms."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((broker_config["host"], 9092))
    except Exception as e:
        pytest.skip(f"Kafka protocol server not running on port 9092: {e}")
    try:
        sock.sendall(make_request(API_SASL_HANDSHAKE, 1, 303, "pytest", pack_string("PLAIN")))
        read_response(sock)
        token = b"\x00Admin\x00Admin"
        sock.sendall(make_request(API_SASL_AUTHENTICATE, 1, 304, "pytest", struct.pack('>i', len(token)) + token))
        resp = read_response(sock)
        corr, err = struct.unpack_from('>ih', resp, 0)
        assert corr == 304
        if err == 0:
            # correlation(4) + error(2) + null message(2) + empty auth bytes(4) + session_lifetime_ms(8)
            assert len(resp) == 4 + 2 + 2 + 4 + 8
    finally:
        sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_partition_not_zero_returns_unknown_topic_or_partition(broker_config):
    """Partition != 0 returns error 3 on Produce, Fetch, ListOffsets; OffsetCommit of an unknown topic too."""
    with kafka_stream_server(broker_config):
        sock = connect_stream_server(broker_config)
        try:
            assert produce_error(sock, 401, STORE_STREAM, 1, b"pytest/store/p1", b"x") == 3

            result = fetch(sock, 402, STORE_STREAM, partitions=[1])
            assert result[STORE_STREAM][0][1] == 3

            err, _ = latest_offset(sock, 403, STORE_STREAM, partition=1)
            assert err == 3

            # Fetch of an unknown topic stays empty with error 0
            result = fetch(sock, 404, "pytest_no_such_stream")
            assert result["pytest_no_such_stream"][0][1] == 0
            assert result["pytest_no_such_stream"][0][3] == []

            # OffsetCommit v1 (with commit timestamp) for an unknown topic
            payload = pack_string("pytest-group") + struct.pack('>i', -1) + pack_string("")
            payload += struct.pack('>i', 1) + pack_string("pytest_no_such_stream") + struct.pack('>i', 1)
            payload += struct.pack('>iqq', 0, 5, -1) + pack_string("")
            sock.sendall(make_request(8, 1, 405, "pytest", payload))
            resp = read_response(sock)
            offset = 8
            _, offset = unpack_string(resp, offset)
            offset += 4
            pid, err = struct.unpack_from('>ih', resp, offset)
            assert (pid, err) == (0, 3)
        finally:
            sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_produce_is_stored_once_and_routed(broker_config):
    """Produce stores once (no double storage via the MQTT publish) and respects stream routing."""
    with kafka_stream_server(broker_config):
        sock = connect_stream_server(broker_config)
        try:
            marker = f"once-{time.time()}".encode()
            # Topic = stream "pytest_store"; the MQTT topic also matches the filter of that stream
            assert produce_error(sock, 501, STORE_STREAM, 0, b"pytest/store/a", marker) == 0
            time.sleep(0.6)  # let the orchestrator's batch flush run
            msgs = fetch(sock, 502, STORE_STREAM)[STORE_STREAM][0][3]
            assert len([m for m in msgs if m[2] == marker]) == 1, "record stored zero or several times"

            # Kafka topic "pytest_other" with an MQTT topic of the other stream: stored once in each stream
            marker2 = f"both-{time.time()}".encode()
            assert produce_error(sock, 503, OTHER_STREAM, 0, b"pytest/store/b", marker2) == 0
            time.sleep(0.6)
            in_other = fetch(sock, 504, OTHER_STREAM)[OTHER_STREAM][0][3]
            in_store = fetch(sock, 505, STORE_STREAM)[STORE_STREAM][0][3]
            assert len([m for m in in_other if m[2] == marker2]) == 1
            assert len([m for m in in_store if m[2] == marker2]) == 1
        finally:
            sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_fetch_high_watermark_is_next_offset(broker_config):
    """The Fetch high watermark equals the ListOffsets latest offset and several partitions share one topic entry."""
    with kafka_stream_server(broker_config):
        sock = connect_stream_server(broker_config)
        try:
            assert produce_error(sock, 601, STORE_STREAM, 0, b"pytest/store/hw", b"hw") == 0
            time.sleep(0.3)
            err, latest = latest_offset(sock, 602, STORE_STREAM)
            assert err == 0

            result = fetch(sock, 603, STORE_STREAM, partitions=[0, 1])
            assert list(result.keys()) == [STORE_STREAM]          # one topic entry ...
            parts = result[STORE_STREAM]
            assert [p[0] for p in parts] == [0, 1]                # ... with both partitions
            assert parts[0][2] == latest
            assert parts[0][3][-1][0] == latest - 1               # last record is just below the watermark
            assert parts[1][1] == 3
        finally:
            sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_produce_acks_zero_sends_no_response(broker_config):
    """acks=0 gets no response; the next response on the connection belongs to the next request."""
    with kafka_stream_server(broker_config):
        sock = connect_stream_server(broker_config)
        try:
            sock.sendall(produce_request(701, STORE_STREAM, 0, legacy_message(b"pytest/store/acks0", b"a0"), acks=0))
            sock.sendall(make_request(API_API_VERSIONS, 0, 702, "pytest", b""))
            resp = read_response(sock)
            assert struct.unpack_from('>i', resp, 0)[0] == 702
        finally:
            sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_produce_unsupported_codec(broker_config):
    """A codec other than none/gzip is rejected with 76 (UNSUPPORTED_COMPRESSION_TYPE)."""
    with kafka_stream_server(broker_config):
        sock = connect_stream_server(broker_config)
        try:
            assert produce_error(sock, 801, STORE_STREAM, 0, b"pytest/store/snappy", b"x", attributes=2) == 76
        finally:
            sock.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_long_poll_wakes_up_when_stream_name_differs_from_filter(broker_config):
    """A waiting Fetch on the stream is answered right after a Produce, not after max_wait_ms."""
    with kafka_stream_server(broker_config):
        consumer = connect_stream_server(broker_config)
        producer = connect_stream_server(broker_config)
        try:
            err, latest = latest_offset(consumer, 901, STORE_STREAM)
            marker = f"wake-{time.time()}".encode()

            def produce_later():
                time.sleep(0.5)
                produce_error(producer, 902, STORE_STREAM, 0, b"pytest/store/wake", marker)

            t = threading.Thread(target=produce_later)
            t.start()
            started = time.time()
            result = fetch(consumer, 903, STORE_STREAM, fetch_offset=latest, max_wait=8000)
            elapsed = time.time() - started
            t.join()
            assert any(m[2] == marker for m in result[STORE_STREAM][0][3])
            assert elapsed < 4.0, f"consumer waited out max_wait_ms ({elapsed:.1f}s)"
        finally:
            consumer.close()
            producer.close()


@pytest.mark.skipif(os.getenv("SKIP_KAFKA_SERVER", "0") == "1", reason="Kafka Server tests skipped")
def test_kafka_servers_graphql_returns_advertised_address(broker_config):
    """kafkaServers returns advertisedHost / advertisedPort."""
    import requests
    with kafka_stream_server(broker_config) as (graphql_url, headers):
        query = "{ kafkaServers { name advertisedHost advertisedPort } }"
        resp = requests.post(graphql_url, json={"query": query}, headers=headers, timeout=5)
        resp.raise_for_status()
        result = resp.json()
        assert "errors" not in result, result
        server = next(s for s in result["data"]["kafkaServers"] if s["name"] == STREAM_SERVER_NAME)
        assert server["advertisedHost"] == "kafka.example.test"
        assert server["advertisedPort"] == 19094
