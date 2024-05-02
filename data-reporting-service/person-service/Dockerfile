FROM amazoncorretto:17 as builder
#Copy sources
COPY data-reporting-service/person-service /usr/src/data-reporting-service/person-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew

#cd to person-service
WORKDIR /usr/src/data-reporting-service/person-service

#Build person service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/person-service/build/libs/person-service*.jar person-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "person-service.jar"]
CMD ["java", "-jar", "person-service.jar"]