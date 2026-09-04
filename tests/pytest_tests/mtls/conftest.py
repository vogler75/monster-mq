"""
Fixtures for mutual TLS (client certificate) tests.

These tests need a broker listening on a TLS port with client certificate
authentication enabled, for example:

    TCPS: 8883
    UserManagement:
      Enabled: true
    SSL:
      KeyStoreType: PEM
      KeyStorePath: server.crt
      KeyPath: server.key
      ClientAuth: REQUEST
      TrustStoreType: PEM
      TrustStorePath: ca.crt
      UseIdentityAsUsername: true
      AutoCreateUser: true

Environment variables:
  MQTT_MTLS_PORT     TLS port of the broker (default: 8883)
  MTLS_CA_CERT       CA certificate that signed the broker and client certificates
  MTLS_CLIENT_CERT   client certificate to present
  MTLS_CLIENT_KEY    private key for the client certificate
  MTLS_CLIENT_CN     Common Name inside the client certificate (default: test-client)
  MTLS_TLS_INSECURE  set to 1 to skip broker hostname verification (default: 1)
  MTLS_AUTO_CREATE_USER  1 if the broker runs with SSL.AutoCreateUser enabled, 0 if
                     disabled. The tests for those two modes are skipped unless this
                     is set, since a broker can only run in one of them at a time.
  GRAPHQL_URL        GraphQL endpoint used to manage accounts (default: http://localhost:4000/graphql)

The whole group is skipped when the certificate paths are not provided.
"""
import os
import pytest


@pytest.fixture(scope="session")
def mtls_config():
    ca = os.getenv("MTLS_CA_CERT")
    cert = os.getenv("MTLS_CLIENT_CERT")
    key = os.getenv("MTLS_CLIENT_KEY")

    missing = [name for name, value in
               (("MTLS_CA_CERT", ca), ("MTLS_CLIENT_CERT", cert), ("MTLS_CLIENT_KEY", key))
               if not value]
    if missing:
        pytest.skip("mTLS tests need " + ", ".join(missing))

    for name, path in (("MTLS_CA_CERT", ca), ("MTLS_CLIENT_CERT", cert), ("MTLS_CLIENT_KEY", key)):
        if not os.path.isfile(path):
            pytest.skip("{} does not point at a file: {}".format(name, path))

    return {
        "host": os.getenv("MQTT_BROKER", "localhost"),
        "port": int(os.getenv("MQTT_MTLS_PORT", "8883")),
        "ca_cert": ca,
        "client_cert": cert,
        "client_key": key,
        "common_name": os.getenv("MTLS_CLIENT_CN", "test-client"),
        "insecure": os.getenv("MTLS_TLS_INSECURE", "1") == "1",
    }
