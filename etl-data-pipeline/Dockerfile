FROM amazoncorretto:17 as builder

#Copy project config
COPY gradle /usr/src/etldatapipeline/gradle
COPY gradlew /usr/src/etldatapipeline/gradlew
COPY build.gradle /usr/src/etldatapipeline/build.gradle
COPY settings.gradle /usr/src/etldatapipeline/settings.gradle

#Copy sources
COPY . /usr/src/etldatapipeline/etl-data-pipeline

WORKDIR /usr/src/etldatapipeline

#Build ETL service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon

FROM amazoncorretto:17

COPY --from=builder /usr/src/etldatapipeline/build/libs/etl-data-pipeline*.jar etl-data-pipeline.jar

# Run jar
ENTRYPOINT ["java", "-jar", "etl-data-pipeline.jar"]
CMD ["java", "-jar", "etl-data-pipeline.jar"]