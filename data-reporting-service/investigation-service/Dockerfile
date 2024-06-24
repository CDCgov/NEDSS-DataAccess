FROM amazoncorretto:17 as builder

#Copy project config
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew
COPY build.gradle /usr/src/data-reporting-service/build.gradle
COPY settings.gradle /usr/src/data-reporting-service/settings.gradle

#Copy sources
COPY data-reporting-service/common-util /usr/src/data-reporting-service/common-util
COPY data-reporting-service/investigation-service /usr/src/data-reporting-service/investigation-service

#cd to investigation-service
WORKDIR /usr/src/data-reporting-service

#Build investigation service along with any required libraries
RUN ./gradlew :investigation-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/investigation-service/build/libs/investigation-service*.jar investigation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "investigation-service.jar"]
CMD ["java", "-jar", "investigation-service.jar"]