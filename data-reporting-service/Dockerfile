FROM amazoncorretto:17 as builder
#Copy project config
COPY gradle /usr/src/etldatapipeline/gradle
COPY gradlew /usr/src/etldatapipeline/gradlew
COPY build.gradle /usr/src/etldatapipeline/build.gradle
COPY settings.gradle /usr/src/etldatapipeline/settings.gradle
#Copy sources
COPY . /usr/src/etldatapipeline/data-reporting-service
#COPY . /usr/src/etldatapipeline
WORKDIR /usr/src/etldatapipeline/data-reporting-service
#Build ETL service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/etldatapipeline/data-reporting-service/build/libs/data-reporting-service*.jar data-reporting-service.jar
# Run jar
ENTRYPOINT ["java", "-jar", "data-reporting-service.jar"]
CMD ["java", "-jar", "data-reporting-service.jar"]