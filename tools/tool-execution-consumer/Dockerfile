FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -q
COPY src ./src
RUN mvn package -q -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/tool-execution-consumer-0.1.0-SNAPSHOT.jar app.jar

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:29092
ENV WEB_SEARCH_URL=http://tool-service:8082/api/tools/web-search
ENV CONTACTS_URL=http://tool-service:8082/api/tools/contacts
ENV CALENDAR_URL=http://tool-service:8082/api/tools/calendar
ENV EMAIL_URL=http://tool-service:8082/api/tools/email
ENV TASKS_URL=http://tool-service:8082/api/tools/tasks

ENTRYPOINT ["java", "-jar", "app.jar"]
