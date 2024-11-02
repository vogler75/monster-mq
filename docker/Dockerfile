FROM amazoncorretto:17-alpine
#FROM eclipse-temurin:21
ADD entrypoint.sh server-keystore.jks target/broker-1.0-SNAPSHOT.jar target/dependencies /app/
RUN chmod +x /app/entrypoint.sh
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
CMD []
