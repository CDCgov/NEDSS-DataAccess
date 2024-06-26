FROM amazoncorretto:17 as builder
#Copy sources
COPY data-reporting-service /usr/src/data-reporting-service

#cd to ldfdata-service
WORKDIR /usr/src/data-reporting-service/ldfdata-service

#Build ldfdata service along with any required libraries
RUN ./gradlew :ldfdata-service:buildNeeded  -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/data-reporting-service/ldfdata-service/build/libs/ldfdata-service*.jar ldfdata-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "ldfdata-service.jar"]
CMD ["java", "-jar", "ldfdata-service.jar"]