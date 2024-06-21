FROM amazoncorretto:17 as builder

#Copy sources
COPY common-util /usr/src/data-reporting-service/common-util
COPY data-reporting-service/investigation-service /usr/src/data-reporting-service/investigation-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew

#cd to investigation-service
WORKDIR /usr/src/data-reporting-service/investigation-service

#Build investigation service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/investigation-service/build/libs/investigation-service*.jar investigation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "investigation-service.jar"]
CMD ["java", "-jar", "investigation-service.jar"]