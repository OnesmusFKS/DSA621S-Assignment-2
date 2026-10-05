FROM ballerina/ballerina:2201.13.5 AS build
USER root
WORKDIR /app
COPY . .
RUN bal build

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/bin/delivery_service.jar app.jar
EXPOSE 9005
CMD ["java", "-jar", "app.jar"]
