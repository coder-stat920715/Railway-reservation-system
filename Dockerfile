# Shared multi-stage Dockerfile: docker build --build-arg MODULE=<module-name> .
FROM maven:3.9-eclipse-temurin-21 AS build
ARG MODULE
WORKDIR /build
COPY . .
RUN mvn -B -q -pl ${MODULE} -am -DskipTests package

FROM eclipse-temurin:21-jre
ARG MODULE
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /build/${MODULE}/target/${MODULE}-1.0.0.jar /app/app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
