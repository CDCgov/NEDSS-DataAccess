FROM amazoncorretto:17 as builder
#Copy sources
COPY common-util /usr/src/common-util
COPY observation-service /usr/src/observation-service

#cd to observation-service
WORKDIR /usr/src/

#Build person service along with any required libraries
RUN ./gradlew :observation-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/observation-service/build/libs/observation-service*.jar investigation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "observation-service.jar"]
CMD ["java", "-jar", "observation-service.jar"]