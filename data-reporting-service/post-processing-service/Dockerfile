# Build the post processing service docker image
FROM amazoncorretto:17 as builder
#Copy sources
COPY data-reporting-service/common-util /usr/src/data-reporting-service/common-util
COPY data-reporting-service/post-processing-service /usr/src/data-reporting-service/post-processing-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew

#cd to post-processing-service
WORKDIR /usr/src/data-reporting-service/post-processing-service

#Build post-processing service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/post-processing-service/build/libs/post-processing-service*.jar post-processing-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "post-processing-service.jar"]
CMD ["java", "-jar", "post-processing-service.jar"]