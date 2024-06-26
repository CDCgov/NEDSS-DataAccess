FROM amazoncorretto:17 as builder

#Copy sources
COPY common-util /usr/src/data-reporting-service/common-util
COPY organization-service /usr/src/data-reporting-service/organization-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew

#Set the Working Directory
WORKDIR /usr/src/data-reporting-service/organization-service

#Build organization service along with any required libraries
RUN ./gradlew :organization-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/organization-service/build/libs/organization-service*.jar organization-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "organization-service.jar"]
CMD ["java", "-jar", "organization-service.jar"]