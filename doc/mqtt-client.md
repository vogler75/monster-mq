# MQTT Client Bridge

The MQTT client bridge (device type `MQTT-Client`) connects MonsterMQ to a remote MQTT broker. Each bridge has a list of topic mappings:

- `SUBSCRIBE` mappings pull `remoteTopic` from the remote broker into `localTopic`.
- `PUBLISH` mappings push local `localTopic` messages to the remote `remoteTopic`.

Bridges are managed through GraphQL (`mqttClient { create / update / delete / start / stop }`) or the dashboard (**MQTT Clients**). Each bridge runs on the cluster node given by `nodeId`. The edge broker has the same bridge with the same GraphQL fields.

The `brokerUrl` scheme selects the transport:

| Scheme | Transport | Default port |
|---|---|---|
| `tcp://` | plain MQTT | 1883 |
| `ssl://` | MQTT over TLS | 8883 |
| `ws://` | MQTT over WebSocket | 80 |
| `wss://` | MQTT over TLS WebSocket | 443 |

## TLS and mutual TLS

With `ssl://` or `wss://` the bridge verifies the remote server certificate against the JVM default trust store. Set `sslVerifyCertificate: false` only for testing: it disables both certificate and hostname checks.

For private CAs, client certificates (mutual TLS) and cloud brokers such as AWS IoT Core, use these optional fields:

| Field | Description |
|---|---|
| `tlsCaCertPath` | PEM file with one or more trusted CA certificates. When set, it **replaces** the default trust store. |
| `tlsClientCertPath` | Client certificate: a PEM certificate (chain) or, with `tlsClientKeyFormat: PKCS12`, a PKCS#12 bundle (`.p12` / `.pfx`). |
| `tlsClientKeyPath` | PEM private key for the certificate: PKCS#1 (`RSA PRIVATE KEY`), PKCS#8 (`PRIVATE KEY`), SEC1 EC (`EC PRIVATE KEY`) or encrypted PKCS#8 / legacy OpenSSL encryption. Not used for PKCS12. |
| `tlsClientKeyPassword` | Password of an encrypted key or the PKCS12 bundle. It is write-only: typed queries return only `tlsClientKeyPasswordSet`. On update, `null` keeps the stored password and `""` clears it. |
| `tlsClientKeyFormat` | `PEM` (default) or `PKCS12`. |
| `tlsAlpnProtocols` | ALPN protocol names sent in the TLS handshake, e.g. `["x-amzn-mqtt-ca"]`. |
| `tlsServerName` | Server name sent as SNI and used for hostname verification. Defaults to the host in `brokerUrl`. Use it when you connect by IP address or through a tunnel. |

All paths refer to files on the broker host. Each bridge reads them on the node that runs it.

When updating a bridge, omitted or `null` TLS paths, ALPN protocols, and server name keep their stored values. Send `""` to clear a path or server name, and `[]` to clear ALPN protocols. Clearing the client certificate also removes its stored key password. To switch a bridge to `tcp://` or `ws://`, clear its TLS paths, ALPN protocols, and server name in the same update.

The generic `getDevices` export also omits `tlsClientKeyPassword`. After importing an encrypted key or PKCS12 bundle, set its password again before enabling the bridge. Device export requires an administrator account when user management is enabled.

### Validation

Create and update reject an invalid combination, and nothing is saved:

- `tls*` settings with a `tcp://` or `ws://` URL. A stored key password on its own is ignored.
- PEM: a certificate without a key, or a key without a certificate.
- PKCS12: a missing bundle path, or a `tlsClientKeyPath` (the key is inside the bundle).
- An unknown `tlsClientKeyFormat`, or an ALPN entry that is empty, longer than 255 bytes or not printable ASCII.

The files are read when the bridge starts. Problems such as a missing file, a wrong password or a key that does not match the certificate stop the bridge with an error naming the field and path, for example:

```
Invalid MQTT client TLS configuration: tlsClientKeyPath '/etc/monstermq/certs/client.key': private key is encrypted but no tlsClientKeyPassword is set
```

The bridge does not retry in that case; fix the configuration and start it again.

### Example: private CA with a client certificate

A remote Mosquitto broker with `require_certificate true`, signed by your own CA:

```graphql
mutation {
  mqttClient {
    create(input: {
      name: "plant-bridge"
      namespace: "plant"
      nodeId: "*"
      config: {
        brokerUrl: "ssl://mqtt.plant.example.com:8883"
        clientId: "monstermq-plant"
        tlsCaCertPath: "/etc/monstermq/certs/ca.pem"
        tlsClientCertPath: "/etc/monstermq/certs/client.pem"
        tlsClientKeyPath: "/etc/monstermq/certs/client.key"
        tlsClientKeyPassword: "key-passphrase"   # only for encrypted keys
      }
    }) { success errors }
  }
}
```

With a PKCS#12 bundle instead of separate PEM files:

```graphql
config: {
  brokerUrl: "ssl://mqtt.plant.example.com:8883"
  tlsCaCertPath: "/etc/monstermq/certs/ca.pem"
  tlsClientKeyFormat: "PKCS12"
  tlsClientCertPath: "/etc/monstermq/certs/client.p12"
  tlsClientKeyPassword: "bundle-password"
}
```

Topic mappings are added with `mqttClient { addAddress(...) }` or in the dashboard, as for any other bridge.

### Example: AWS IoT Core

AWS IoT Core authenticates devices with X.509 client certificates. You need the device certificate, its private key and the Amazon Root CA (`AmazonRootCA1.pem`). The endpoint is shown under **AWS IoT > Settings** (`<prefix>-ats.iot.<region>.amazonaws.com`). The `clientId` must be allowed by the policy attached to the certificate.

Port 8883:

```graphql
config: {
  brokerUrl: "ssl://a1b2c3d4e5f6g7-ats.iot.eu-central-1.amazonaws.com:8883"
  clientId: "monstermq-gateway-1"
  tlsCaCertPath: "/etc/monstermq/aws/AmazonRootCA1.pem"
  tlsClientCertPath: "/etc/monstermq/aws/device.pem.crt"
  tlsClientKeyPath: "/etc/monstermq/aws/private.pem.key"
}
```

Port 443, for networks that only allow HTTPS egress. AWS routes MQTT with client certificates on 443 by the ALPN protocol `x-amzn-mqtt-ca`:

```graphql
config: {
  brokerUrl: "ssl://a1b2c3d4e5f6g7-ats.iot.eu-central-1.amazonaws.com:443"
  clientId: "monstermq-gateway-1"
  tlsCaCertPath: "/etc/monstermq/aws/AmazonRootCA1.pem"
  tlsClientCertPath: "/etc/monstermq/aws/device.pem.crt"
  tlsClientKeyPath: "/etc/monstermq/aws/private.pem.key"
  tlsAlpnProtocols: ["x-amzn-mqtt-ca"]
}
```

`tests/device-configs-examples.json` contains this configuration as a stored device (`aws-iot1`).

### Limitations

- Certificate files must exist on every node that may run the bridge. With `nodeId: "*"`, that is every cluster node.
- Files are read once at start. After rotating a certificate, stop and start the bridge, or save it again.
- The dashboard sets paths only; it does not upload certificate files.
- WebSocket connections to AWS IoT Core need SigV4 or a custom authorizer, which the bridge does not support. Use `ssl://` for AWS.
- The bridge uses MQTT 3.1.1. The MQTT v5 fields are stored but not used yet.
