FROM amazoncorretto:17 as builder

#Copy sources
COPY data-reporting-service /usr/src/data-reporting-service
#COPY . /usr/src/etldatapipeline
WORKDIR /usr/src/etldatapipeline/data-reporting-service
#Build ETL service along with any required libraries
RUN ./gradlew buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/etldatapipeline/data-reporting-service/build/libs/data-reporting-service*.jar data-reporting-service.jar
# Run jar
ENTRYPOINT ["java", "-jar", "data-reporting-service.jar"]
CMD ["java", "-jar", "data-reporting-service.jar"]