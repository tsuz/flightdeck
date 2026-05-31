FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
# Skip test compilation+execution for the image build (tests run in CI via
# `mvn test`); keeps the image build decoupled from test sources.
RUN mvn package -q -Dmaven.test.skip=true

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/flightdeck-streams-0.1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV MEMOIR_ENABLED=true
ENV MEMOIR_SESSION_INACTIVITY_THRESHOLD_SECONDS=20
ENV MEMOIR_SESSION_PUNCTUATE_INTERVAL_SECONDS=5

ENTRYPOINT ["java", "-jar", "app.jar"]
