FROM amazoncorretto:17 as builder
#Copy project config
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew
COPY build.gradle /usr/src/data-reporting-service/build.gradle
COPY settings.gradle /usr/src/data-reporting-service/settings.gradle
#Copy sources
COPY . /usr/src/data-reporting-service/person-service
#COPY . /usr/src/etldatapipeline
WORKDIR /usr/src/data-reporting-service/person-service
#Build ETL service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/person-service/build/libs/person-service*.jar person-service.jar
# Run jar
ENTRYPOINT ["java", "-jar", "person-service.jar"]
CMD ["java", "-jar", "person-service.jar"]