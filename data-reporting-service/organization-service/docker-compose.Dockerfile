FROM amazoncorretto:17 as builder

#Copy sources
COPY . /usr/src/data-reporting-service/organization-service
COPY gradle /usr/src/data-reporting-service/gradle
COPY gradlew /usr/src/data-reporting-service/gradlew

#COPY . /usr/src/etldatapipeline
WORKDIR /usr/src/data-reporting-service/organization-service

#Build organization service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/organization-service/build/libs/organization-service*.jar organization-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "organization-service.jar"]
CMD ["java", "-jar", "organization-service.jar"]