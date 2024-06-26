FROM amazoncorretto:17 as builder
#Copy sources
COPY data-reporting-service /usr/src/data-reporting-service

#cd to investigation-service
WORKDIR /usr/src/data-reporting-service/investigation-service

#Build investigation service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/investigation-service/build/libs/investigation-service*.jar investigation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "investigation-service.jar"]
CMD ["java", "-jar", "investigation-service.jar"]