FROM maven:3.9.12-eclipse-temurin-17@sha256:a0603aab698040d9c94259f379ec0487da1678560748d6c7508483034033c53d
WORKDIR /app
COPY backend/target/backend-0.1.0.jar /app/backend.jar
COPY spark-engine/target/spark-engine-0.1.0.jar /app/engine/spark-engine-0.1.0.jar
COPY spark-engine/target/dependency /app/engine/dependency
RUN mkdir -p /data && chown -R 1000:1000 /data /app
USER 1000:1000
ENTRYPOINT ["java","-Xmx512m","-jar","/app/backend.jar"]
