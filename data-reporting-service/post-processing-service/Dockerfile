# Build the post processing service docker image
FROM amazoncorretto:17 as builder
#Copy sources
COPY data-reporting-service /usr/src/data-reporting-service

#Set the Working Directory
WORKDIR /usr/src/data-reporting-service/

#Build post-processing service along with any required libraries
RUN ./gradlew :post-processing-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/post-processing-service/build/libs/post-processing-service*.jar post-processing-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "post-processing-service.jar"]
CMD ["java", "-jar", "post-processing-service.jar"]