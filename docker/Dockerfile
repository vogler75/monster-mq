FROM amazoncorretto:21-alpine
#FROM eclipse-temurin:21
ADD entrypoint.sh server-keystore.jks broker-1.0-SNAPSHOT.jar dependencies /app/
RUN chmod +x /app/entrypoint.sh
WORKDIR /app
ENTRYPOINT ["/app/entrypoint.sh"]
CMD []
