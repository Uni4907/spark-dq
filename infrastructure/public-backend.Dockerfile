FROM maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d AS build
WORKDIR /src
COPY pom.xml ./
COPY backend/pom.xml backend/pom.xml
COPY spark-engine/pom.xml spark-engine/pom.xml
COPY backend/src backend/src
COPY spark-engine/src spark-engine/src
# Retry once in the same fresh build stage so a transient download can recover.
RUN mvn -B -DskipTests package || mvn -B -U -DskipTests package

FROM maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d
WORKDIR /app
COPY --from=build /src/backend/target/backend-0.1.0.jar /app/backend.jar
COPY --from=build /src/spark-engine/target/spark-engine-0.1.0.jar /app/engine/spark-engine-0.1.0.jar
COPY --from=build /src/spark-engine/target/dependency /app/engine/dependency
RUN mkdir -p /data && chown -R 1000:1000 /data /app
USER 1000:1000
ENTRYPOINT ["java","-Xmx512m","-jar","/app/backend.jar"]
