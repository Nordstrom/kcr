FROM gradle:5.4.1-jdk11 as gradle

RUN apt-get update && \
    apt-get install -y git && \
    git clone https://github.com/Nordstrom/kcr.git && \
    cd kcr && \
    gradle clean build

FROM openjdk:11-jdk-slim

COPY --from=gradle /home/gradle/kcr/build/libs/kcr.jar /usr/app/kcr.jar

WORKDIR /usr/app

ENTRYPOINT ["java", "-jar", "kcr.jar"]