FROM openjdk:8

ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/temperature-sensor/temperature-sensor.jar"]

# Add Maven dependencies
ADD target/lib /usr/share/temperature-sensor/lib
ARG JAR_FILE
ADD target/${JAR_FILE} /usr/share/temperature-sensor/temperature-sensor.jar
