FROM amazoncorretto:17 as builder
#Copy sources
COPY post-processing-service /usr/src/post-processing-service

#cd to root directory
WORKDIR /usr/src/

#Build post-processing service along with any required libraries
RUN ./gradlew :post-processing-service:buildNeeded -x test --no-daemon
FROM amazoncorretto:17
COPY --from=builder /usr/src/post-processing-service/build/libs/post-processing-service*.jar post-processing-service.jar

# Run jar
ENTRYPOINT ["java", "-jar", "post-processing-service.jar"]
CMD ["java", "-jar", "post-processing-service.jar"]