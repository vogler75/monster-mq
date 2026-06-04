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
    request = make_request(API_SASL_HANDSHAKE, 0, 9999, "auth-helper", handshake_payload)
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
    # 1. Connect MQTT client and publish message to sensors/temp
    mqtt_client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    if broker_config["username"]:
        mqtt_client.username_pw_set(broker_config["username"], broker_config["password"])
    
    mqtt_client.connect(broker_config["host"], broker_config["port"])
    mqtt_client.loop_start()
    
    test_payload = b"{\"value\": 42.0}"
    mqtt_client.publish("sensors/temp", test_payload, qos=1)
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
        request = make_request(17, 0, 1001, "test-auth-client", handshake_payload)
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
        request = make_request(17, 0, 1003, "test-auth-client", handshake_payload)
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
        assert error_code == 58  # SASLAuthenticationFailed (58)
        
        # Verify connection is subsequently closed by server
        data = sock.recv(1024)
        assert len(data) == 0  # Socket EOF
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


