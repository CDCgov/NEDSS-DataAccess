FROM amazoncorretto:17 as builder
#Copy project config
COPY gradle /usr/src/datareportingservice/gradle
COPY gradlew /usr/src/datareportingservice/gradlew
COPY build.gradle /usr/src/datareportingservice/build.gradle
COPY settings.gradle /usr/src/datareportingservice/settings.gradle
#Copy sources
COPY . /usr/src/datareportingservice/data-reporting-service
#COPY . /usr/src/datareportingservice
WORKDIR /usr/src/datareportingservice/data-reporting-service
#Build ETL service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/datareportingservice/data-reporting-service/build/libs/data-reporting-service*.jar data-reporting-service.jar
# Run jar
ENTRYPOINT ["java", "-jar", "data-reporting-service.jar"]
CMD ["java", "-jar", "data-reporting-service.jar"]