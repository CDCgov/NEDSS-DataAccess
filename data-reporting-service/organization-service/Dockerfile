FROM amazoncorretto:17 as builder

#Copy sources
COPY data-reporting-service /usr/src/data-reporting-service

#COPY . /usr/src/etldatapipeline
WORKDIR /usr/src/data-reporting-service/organization-service

#Build organization service along with any required libraries
RUN ./gradlew :organization-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/organization-service/build/libs/organization-service*.jar organization-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "organization-service.jar"]
CMD ["java", "-jar", "organization-service.jar"]