FROM ballerina/ballerina:2201.13.5 AS build
USER root
WORKDIR /app
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/bin/order_service.jar app.jar
EXPOSE 9003
# Config comes from env vars / a mounted Config.toml (BAL_CONFIG_FILES), not baked into the image
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=5 \
  CMD curl -fs http://localhost:9003/orders/health || exit 1
CMD ["java", "-jar", "app.jar"]
