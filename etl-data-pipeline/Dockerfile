FROM amazoncorretto:17 as builder


#Copy project config
COPY etl-data-pipeline/gradle /usr/src/datapipeline/gradle
COPY etl-data-pipeline/gradlew /usr/src/datapipeline/gradlew
COPY etl-data-pipeline/build.gradle /usr/src/datapipeline/build.gradle
COPY etl-data-pipeline/settings.gradle /usr/src/datapipeline/settings.gradle

#Copy sources
COPY etl-data-pipeline /usr/src/datapipeline/etl-data-pipeline

WORKDIR /usr/src/datapipeline

#Build ETL service along with any required libraries
RUN ./gradlew :etl-data-pipeline:buildNeeded -x test --no-daemon

FROM amazoncorretto:17

COPY --from=builder /usr/src/datapipeline/etl-data-pipeline/build/libs/etl-data-pipeline*.jar etl-data-pipeline.jar

# Run jar
ENTRYPOINT ["java", "-jar", "etl-data-pipeline.jar"]
CMD ["java", "-jar", "etl-data-pipeline.jar"]