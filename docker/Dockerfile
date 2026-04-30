ARG BASE_IMAGE=ghcr.io/graalvm/jdk-community:25
FROM ${BASE_IMAGE}
ADD entrypoint.sh server-keystore.jks config.yaml target/broker-1.0-SNAPSHOT.jar target/dependencies /app/
RUN chmod +x /app/entrypoint.sh && mkdir -p /app/db
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
CMD []
