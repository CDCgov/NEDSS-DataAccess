FROM amazoncorretto:17 as builder
#Copy sources
COPY common-util /usr/src/data-reporting-service/common-util
COPY person-service /usr/src/data-reporting-service/person-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew
COPY settings.gradle /usr/src/data-reporting-service/settings.gradle

#Set the Working Directory
WORKDIR /usr/src/data-reporting-service/

#Build person service along with any required libraries
RUN ./gradlew :person-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/person-service/build/libs/person-service*.jar person-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "person-service.jar"]
CMD ["java", "-jar", "person-service.jar"]