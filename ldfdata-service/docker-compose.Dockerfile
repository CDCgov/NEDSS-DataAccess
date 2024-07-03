FROM amazoncorretto:17 as builder
#Copy sources
COPY common-util /usr/src/common-util
COPY ldfdata-service /usr/src/ldfdata-service

#cd to ldfdata-service
WORKDIR /usr/src/

#Build person service along with any required libraries
RUN ./gradlew :ldfdata-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/ldfdata-service/build/libs/ldfdata-service*.jar investigation-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "ldfdata-service.jar"]
CMD ["java", "-jar", "ldfdata-service.jar"]