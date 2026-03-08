# GitHub Issue Draft: Add mutual TLS client certificate support to MQTT client

## Title

Add mutual TLS client certificate support to MQTT client for AWS IoT Core compatibility

## Body

## Summary

Extend the existing `MQTT-Client` connector to support client certificate and private key authentication over TLS.

This is the main missing capability needed to connect MonsterMQ's MQTT client bridge to AWS IoT Core using the standard X.509 certificate flow.

## Problem

The current MQTT client bridge supports:
- TCP / SSL / WS / WSS broker URLs
- username/password authentication
- server certificate verification on TLS connections
- insecure trust-all mode for development

However, it does not currently support:
- client certificate authentication
- private key loading
- custom trust material for CA certificates
- optional ALPN configuration for AWS IoT Core on port 443

That blocks direct interoperability with AWS IoT Core in the common production setup.

## Why

AWS IoT Core commonly expects MQTT clients to authenticate with X.509 client certificates over TLS on port `8883`, or on port `443` with additional connection requirements such as ALPN depending on the endpoint and auth mode.

MonsterMQ already has a usable MQTT bridge implementation, so the right first step is to extend that bridge rather than create a separate AWS IoT-specific connector.

Relevant AWS documentation:
- https://docs.aws.amazon.com/iot/latest/developerguide/protocols.html
- https://docs.aws.amazon.com/iot/latest/developerguide/custom-auth.html

## Current Touchpoints

Likely implementation areas:
- `broker/src/main/kotlin/stores/devices/MqttConfig.kt`
- `broker/src/main/kotlin/devices/mqttclient/MqttClientConnector.kt`

Current behavior:
- TLS server verification is configured in `MqttClientConnectionConfig`
- `MqttClientConnector` currently uses either the default JVM SSL socket factory or an insecure trust-all socket factory
- no client keystore, private key, or CA bundle loading exists today

## Proposed Scope

### 1. Extend MQTT client config

Add new optional fields to `MqttClientConnectionConfig`, for example:
- `tlsCaCertPath`
- `tlsClientCertPath`
- `tlsClientKeyPath`
- `tlsClientKeyPassword`
- `tlsClientKeyFormat`
- `tlsAlpnProtocols`
- `tlsServerName`

Notes:
- keep all fields optional so current users are unaffected
- preserve existing username/password mode
- preserve existing `sslVerifyCertificate` behavior

### 2. Build a custom SSLContext

Update `MqttClientConnector` so that for `ssl://` and `wss://` connections it can:
- load trust material from the configured CA certificate path
- load client certificate and private key from disk
- create a proper `SSLContext`
- install the resulting `SSLSocketFactory` into the Paho connection options

This should support:
- default JVM trust store when no CA path is provided
- explicit CA trust store when `tlsCaCertPath` is provided
- mutual TLS when both client certificate and private key are provided

### 3. Optional ALPN support

Investigate and support ALPN configuration where the JVM and socket implementation allow it.

This matters for AWS IoT Core connections on port `443`, where ALPN may be required depending on the protocol and auth mode.

If ALPN support cannot be implemented portably in the first pass:
- document the limitation clearly
- still support the common `8883` mutual TLS path

### 4. Validation

Add config validation rules such as:
- certificate path requires matching private key path for mutual TLS
- reject unsupported key formats
- reject invalid ALPN values
- provide actionable startup errors

### 5. Documentation

Add documentation for:
- generic MQTT client mutual TLS configuration
- AWS IoT Core connection example
- differences between port `8883` and `443`
- limitations around ALPN / WebSockets / custom auth if not implemented

## Suggested Config Shape

Example direction only:

```yaml
config:
  brokerUrl: "ssl://<aws-iot-endpoint>:8883"
  clientId: "monstermq-bridge"
  cleanSession: true
  sslVerifyCertificate: true
  tlsCaCertPath: "/path/AmazonRootCA1.pem"
  tlsClientCertPath: "/path/device-certificate.pem.crt"
  tlsClientKeyPath: "/path/private.pem.key"
  addresses:
    - mode: SUBSCRIBE
      remoteTopic: "devices/+/telemetry"
      localTopic: "aws/in/devices/+"
      qos: 1
    - mode: PUBLISH
      remoteTopic: "devices/${topic}"
      localTopic: "local/devices/#"
      qos: 1
```

The exact field names can be adjusted to match repo conventions.

## Non-Goals for v1

- creating a separate `AWSIOT-Client` device type
- SigV4 WebSocket auth
- custom authorizers
- fleet provisioning
- shadow / jobs UI abstractions
- automatic certificate provisioning

## Acceptance Criteria

- the existing `MQTT-Client` can connect to a broker using mutual TLS client certificate auth
- CA certificate, client certificate, and private key can be configured per device
- startup validation reports configuration errors clearly
- existing non-mTLS MQTT client configs continue to work unchanged
- documentation includes an AWS IoT Core example
- at least one integration test path exists for certificate-based TLS connections

## Testing Notes

Prefer one of:
- a TLS test broker with client certificate auth in integration tests
- a documented manual verification path against AWS IoT Core

If AWS IoT Core is not practical for CI, use a local MQTT broker configured for mutual TLS and keep AWS IoT verification as a manual test recipe.

## Follow-Up Work

Possible later follow-ups:
- ALPN-specific support improvements for port `443`
- `AWSIOT-Client` convenience wrapper around `MQTT-Client`
- helpers for AWS reserved topics such as shadows and jobs
