#FROM amazoncorretto:21-alpine
FROM ghcr.io/graalvm/jdk-community:25
ADD entrypoint.sh server-keystore.jks target/broker-1.0-SNAPSHOT.jar target/dependencies /app/
RUN chmod +x /app/entrypoint.sh
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
CMD []
