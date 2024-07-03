FROM amazoncorretto:17 as builder
#Copy sources
COPY common-util /usr/src/common-util
COPY investigation-service /usr/src/investigation-service

#cd to investigation-service
WORKDIR /usr/src/

#Build person service along with any required libraries
RUN ./gradlew :investigation-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/investigation-service/build/libs/investigation-service*.jar investigation-service.jar
# Run jar
ENTRYPOINT ["java", "-jar", "investigation-service.jar"]
CMD ["java", "-jar", "investigation-service.jar"]