# syntax=docker/dockerfile:1

# --- build stage: compile the boot jar ---
FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
COPY src ./src
RUN chmod +x gradlew && ./gradlew --no-daemon clean bootJar -x test

# --- runtime stage: slim JRE, non-root ---
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd -r -u 1001 appuser
COPY --from=build /app/build/libs/*.jar app.jar
USER appuser
EXPOSE 8090
ENTRYPOINT ["java", "-jar", "app.jar"]
