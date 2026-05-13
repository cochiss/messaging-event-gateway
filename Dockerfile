# Etapa 1: Build
FROM maven:3.9-eclipse-temurin-17-alpine AS build
COPY . /src
WORKDIR /src
RUN mvn -B package -DskipTests

# Etapa 2: Run
FROM eclipse-temurin:17-jre-alpine
EXPOSE 8080
COPY --from=build /src/target/messaging-event-gateway-*.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
