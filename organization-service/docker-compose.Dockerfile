FROM amazoncorretto:17 as builder
#Copy sources
COPY common-util /usr/src/common-util
COPY organization-service /usr/src/organization-service

#cd to organization-service
WORKDIR /usr/src/

#Build person service along with any required libraries
RUN ./gradlew :organization-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/organization-service/build/libs/organization-service*.jar investigation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "organization-service.jar"]
CMD ["java", "-jar", "organization-service.jar"]