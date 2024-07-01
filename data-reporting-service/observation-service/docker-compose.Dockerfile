FROM amazoncorretto:17 as builder
#Copy sources
COPY common-util /usr/src/data-reporting-service/common-util
COPY observation-service /usr/src/data-reporting-service/observation-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew

#cd to observation-service
WORKDIR /usr/src/data-reporting-service/observation-service

#Build observation service along with any required libraries
RUN ./gradlew :observation-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/observation-service/build/libs/observation-service*.jar observation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "observation-service.jar"]
CMD ["java", "-jar", "observation-service.jar"]