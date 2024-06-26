FROM amazoncorretto:17 as builder
#Copy sources
COPY data-reporting-service /usr/src/data-reporting-service

#cd to person-service
WORKDIR /usr/src/data-reporting-service/

#Build person service along with any required libraries
RUN ./gradlew :person-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/person-service/build/libs/person-service*.jar person-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "person-service.jar"]
CMD ["java", "-jar", "person-service.jar"]