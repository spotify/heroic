FROM maven:3.5.4-jdk-8 as builder
LABEL maintainer "layer8 <layer8@spotify.com>"

COPY . .
RUN tools/install-repackaged
RUN _JAVA_OPTIONS=-Djdk.net.URLClassPath.disableClassPathURLCheck=true ./gradlew clean assemble


#Final
FROM openjdk:8
LABEL maintainer "layer8 <layer8@spotify.com>"

#ENTRYPOINT ["/usr/bin/heroic.sh"]
EXPOSE 8080
EXPOSE 9190

COPY --from=builder heroic-dist/build/libs/heroic-dist-0.0.1-SNAPSHOT-shaded.jar /usr/share/heroic/heroic.jar
COPY run-heroic.sh /usr/bin/heroic.sh
