FROM maven:3.10.0-eclipse-temurin-17 AS build

WORKDIR /build

COPY api ./api
COPY spark-job ./spark-job

RUN mvn -f api/pom.xml clean package -DskipTests \
    && mvn -f spark-job/pom.xml clean package -DskipTests


FROM apache/spark:3.5.9-scala2.12-java17-ubuntu

USER root

WORKDIR /app

COPY --from=build /build/api/target/*.jar /app/api.jar
COPY --from=build /build/spark-job/target/spark-job-1.0.0.jar /app/spark-job.jar

RUN mkdir -p /warehouse \
    && chown -R spark:spark /app /warehouse

USER spark

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/api.jar"]