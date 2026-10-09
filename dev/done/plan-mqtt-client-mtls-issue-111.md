# Plan: Mutual TLS for the MQTT-Client bridge (issue #111)

**Status (2026-10-09): done for the main broker (`efa61eb5`) and dashboard (dashboard `f4c8e23`). The edge broker part (section 3) is tracked in vogler75/monster-mq-edge#20.**

Scope: main broker (Kotlin), edge broker (Go), dashboard. All three change together so the
GraphQL shape stays identical (AGENTS.md parity rule). This plan is the GraphQL sign-off request.

## 1. Config and GraphQL contract (shared by main + edge)

New optional fields on the MQTT-Client connection config (stored in the existing JSON config
blob, so no DB migration):

| Field | Type | Notes |
|---|---|---|
| `tlsCaCertPath` | String | PEM file, one or more CA certs. If set, it *replaces* the default trust store. |
| `tlsClientCertPath` | String | PEM cert (chain) or PKCS#12 bundle, depending on `tlsClientKeyFormat` |
| `tlsClientKeyPath` | String | PEM private key (PKCS#1, PKCS#8, EC). Unused for PKCS12. |
| `tlsClientKeyPassword` | String | Secret. **Input only**, never returned by queries. |
| `tlsClientKeyFormat` | String | `PEM` (default) or `PKCS12` |
| `tlsAlpnProtocols` | [String!] | e.g. `["x-amzn-mqtt-ca"]` for AWS IoT on port 443 |
| `tlsServerName` | String | SNI + hostname verification override |

GraphQL changes:
- `type MqttClientConnectionConfig`: add all fields except `tlsClientKeyPassword`. Add
  `tlsClientKeyPasswordSet: Boolean!` so the UI can show "(unchanged)".
- `input MqttClientConnectionConfigInput`: add all seven fields (all nullable, no defaults
  except `tlsClientKeyFormat: String = "PEM"`).
- Update semantics: `tlsClientKeyPassword = null` keeps the stored value (same as `password`
  today). An empty string `""` clears it.

Files:
- main: `broker/src/main/resources/schema-queries.graphqls:819`, `schema-mutations.graphqls:820`
- edge: `internal/graphql/schema/schema.graphqls:472` and `:529`, then `make gen`

Validation (both brokers, on create/update and at connector start):
- `tlsClientCertPath` requires `tlsClientKeyPath` for PEM, and vice versa; PKCS12 needs only
  `tlsClientCertPath`.
- `tlsClientKeyFormat` must be `PEM` or `PKCS12`.
- ALPN entries: non-blank, <= 255 bytes, printable ASCII.
- Any `tls*` field with a `tcp://` or `ws://` URL is rejected with an error.
- File existence/readability and parse errors are checked at connector start (the device can
  run on a different cluster node than the one serving GraphQL), and reported as a clear
  device error such as `tlsClientKeyPath '/x/key.pem': not a supported PEM private key`.

## 2. Main broker (Kotlin)

1. `stores/devices/MqttConfig.kt` (`MqttClientConnectionConfig`, lines 140-332)
   - Add the fields with null defaults, and add them to `fromJsonObject` / `toJsonObject`.
     `toJsonObject` writes them only when non-null, so existing configs round-trip unchanged.
   - Extend `validate()` with the rules above.
2. New `devices/mqttclient/MqttClientTls.kt`: builds the `SSLSocketFactory`.
   - Trust:
     - `sslVerifyCertificate=false`: the existing trust-all manager.
     - `tlsCaCertPath` set: `KeyStore` from the PEM certs via `CertificateFactory`.
     - Otherwise: JVM default.
   - Key:
     - PEM: parse with BouncyCastle `PEMParser` + `JcaPEMKeyConverter`. This covers PKCS#1,
       PKCS#8, EC and encrypted keys.
     - PKCS12: `KeyStore.getInstance("PKCS12")`.
     - Either way the result goes into a `KeyManagerFactory`.
   - Wrap the factory like the existing trust-all wrapper (`MqttClientConnector.kt:707-758`).
     Its `configureSocket()` sets `SSLParameters.applicationProtocols` (ALPN, JDK 9+) and
     `serverNames` (SNI). It clears `endpointIdentificationAlgorithm` only when verification is
     off.
   - Replace `createTrustAllSslSocketFactory()`. `MqttClientConnector.kt:306-331` then calls
     `MqttClientTls.buildSocketFactory(config)` for `ssl`/`wss`.
3. `broker/pom.xml`: declare `org.bouncycastle:bcpkix-jdk18on` explicitly. It is already on the
   classpath through Milo, so this pins it without increasing the distribution size.
4. `graphql/MqttClientConfigMutations.kt`:
   - `parseDeviceConfigRequest` (907-946): read the new fields.
   - `updateMqttClient` (176-184): preserve `tlsClientKeyPassword` when it is null.
   - `deviceToMap` (960+): output the new fields and `tlsClientKeyPasswordSet`.
5. `graphql/MqttClientConfigQueries.kt` `deviceToMap` (~103-145): same outputs. Also stop
   putting `password` in the output map (line ~115). The schema already hides it, but it should
   not be in the map at all.
6. Paho v3 checks during implementation:
   - `SSLNetworkModule` does a get-then-set on `SSLParameters` when HTTPS hostname
     verification is enabled, so ALPN and SNI set by the wrapper should survive. Confirm this
     with the integration test.
   - JDK hostname verification uses the requested SNI name, so `tlsServerName` also drives
     the identity check.

## 3. Edge broker (Go)

1. `internal/bridge/mqttclient/connector.go`
   - Add the fields to `Config` (lines 34-49). Also add the missing
     `SslVerifyCertificate *bool 'json:"sslVerifyCertificate"'`: it is stored and returned
     today but ignored, so there is currently no way to skip verification.
   - New `tls.go` with `buildTLSConfig(cfg) (*tls.Config, error)`:
     - `MinVersion` TLS 1.2.
     - `InsecureSkipVerify = !verify`.
     - `RootCAs` from `tlsCaCertPath` using `x509.NewCertPool` + `AppendCertsFromPEM`.
     - `Certificates`: `tls.X509KeyPair` for PEM. Encrypted PKCS#8 keys decrypt via
       `github.com/youmark/pkcs8`; PKCS12 goes through `software.sslmate.com/src/go-pkcs12`.
       Both are pure Go, so the no-CGO rule holds.
     - `NextProtos` = ALPN, `ServerName` = `tlsServerName`.
   - `Start()` (lines 180-182) uses it. A TLS build error sets the device error state and does
     not enter the reconnect loop, because retrying cannot fix a config error.
2. `internal/graphql/resolvers/resolver.go`
   - `mqttClientConfigInputToMap` (:2684) and `mqttClientConfigInputToMergedMap` (:2733):
     preserve the key password.
   - `mapToConnectionConfig` (:2771): add the outputs.
   - Create/Update (:2483/:2499): add validation. Mirror the Kotlin `validate()`, including
     the basic brokerUrl/clientId checks the edge lacks today.
3. Edge dashboard: nothing extra. `make prepare-dashboard` embeds the shared dashboard build.

## 4. Dashboard

In `src/pages/mqtt-client-detail.html` and `src/js/mqtt-client-detail.js`:
- Add a "TLS / Client Certificate" `<h4>` sub-section after the SSL-verify checkbox (html
  :258). It follows the MQTT v5 sub-section pattern and is visible when the broker URL scheme
  is `ssl://` or `wss://`, toggled on URL input. Fields:
  - CA cert path
  - Key format select (PEM / PKCS12); PKCS12 hides the key path field
  - Client cert path
  - Client key path
  - Key password: `type="password"`, `autocomplete="new-password"`, placeholder
    "(unchanged)" when `tlsClientKeyPasswordSet`, sent as null when blank
  - ALPN protocols: comma-separated text, split into an array
  - Server name (SNI)
- Hint text: paths are on the broker host, on the node running the bridge.
- Compatibility with older brokers: copy the `archive-group-detail.js` pattern.
  - Introspect with `getTypeFields('MqttClientConnectionConfig')` and
    `getTypeFields('MqttClientConnectionConfigInput')`.
  - Build the query selection conditionally, hide the section when the fields are missing,
    and delete unsupported keys from the input before mutating.
  - Without this, the fixed query string fails against a broker that lacks the fields.
- Client-side checks: cert and key are both set or both empty for PEM, and the ALPN list is not
  empty after splitting.
- Follow DESIGN.md: shared `.form-group` / `.checkbox-group` components, no local component CSS.

## 5. Tests

- **Main, Kotlin unit tests** (`broker/src/test`):
  - `validate()` rules.
  - `MqttClientTls` with certs generated in-test by BouncyCastle: PKCS#1, PKCS#8, encrypted
    PKCS#8, EC, PKCS12, and a bad path giving a readable error.
- **Main, Python integration test** (`tests/pytest_tests/bridge/test_mqtt_client_mtls.py`):
  - Generate a CA, server cert and client cert, and start Mosquitto (docker) with
    `require_certificate true`.
  - Create the bridge via GraphQL and verify messages flow both ways.
  - Negative case: without a client cert the bridge does not connect.
  - Skip when docker is unavailable.
- **Edge**:
  - Unit tests for `buildTLSConfig`.
  - `test/integration/bridge_mtls_test.go`, reusing `createTestCertificates` /
    `startMTLSBroker` from `mtls_test.go`. The bridge connects to an mTLS edge broker. Also
    test that ALPN is carried to the server via `tls.Config.NextProtos` on the listener, and
    the negative case.
- **Regression**: existing bridge tests (`bridge_test.go`, the Python GraphQL tests) must pass
  unchanged, which proves old configs still work.

## 6. Documentation

- main: new `doc/mqtt-client.md` covering:
  - the bridge overview and all fields;
  - a generic mTLS example with a private CA;
  - AWS IoT Core on 8883, and on 443 with ALPN `x-amzn-mqtt-ca` and the Amazon Root CA;
  - limitations: cert files must exist on each node that may run the device, no hot reload
    on file change (restart or toggle the device), and ALPN over `wss://` is not relevant to
    AWS.
- Link it from `doc/security.md`, and add an example to `tests/device-configs-examples.json`.
- edge: a README section with the same examples and a note on parity.

## 7. Order of work

1. GraphQL contract sign-off (this plan).
2. Main backend, plus unit tests.
3. Edge backend, plus tests.
4. Dashboard.
5. Integration tests and docs.

Commits are separate per repo and are not pushed without approval.

## Out of scope / follow-ups

- Uploading cert/key files through the dashboard (store the PEM content in the config instead
  of paths).
- Hot reload on certificate rotation.
- AWS-specific wrapper device type and SigV4 WebSockets.
- Using `protocolVersion` 5 (still ignored by both connectors today).
- The NATS bridge's "PEM or JKS" CA loader only loads keystores. It could reuse
  `MqttClientTls` later.
