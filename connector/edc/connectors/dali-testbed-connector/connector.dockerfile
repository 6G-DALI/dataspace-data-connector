# Use an official OpenJDK runtime as a parent image
#FROM openjdk:17-jdk
FROM amazoncorretto:17-alpine-jdk

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY build/libs/connector.jar .

# Which build this image is: passed by the deploy workflow, reported by GET /api/health. Declared after the
# jar is copied so a new commit does not invalidate the cached layers above.
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV BUILD_SHA=$GIT_SHA
ENV BUILD_TIME=$BUILD_TIME

# Specify the command to run the application
CMD  java -jar -Dedc.fs.config=connector.properties connector.jar


# Define the mount point for the external directory
# This will be provided at runtime when the docker container is run using the -v flag
VOLUME /data
