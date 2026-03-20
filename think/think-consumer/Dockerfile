FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -q -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/think-consumer-0.1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV CLAUDE_API_KEY=""
ENV CLAUDE_MODEL=claude-sonnet-4-20250514
ENV TOOLS_JSON_FILE=/app/tools.json

ENTRYPOINT ["java", "-jar", "app.jar"]
