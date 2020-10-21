FROM maven:3.6.3-jdk-11 as builder
LABEL maintainer="prism <prism@spotify.com>"

RUN apt-get update && apt-get install -y git
COPY . .
RUN _JAVA_OPTIONS=-Djdk.net.URLClassPath.disableClassPathURLCheck=true ./gradlew clean assemble


# Final Image
FROM openjdk:11
LABEL maintainer="prism <prism@spotify.com>"

EXPOSE 8080
EXPOSE 9190

COPY --from=builder heroic-dist/build/libs/heroic-dist-0.0.1-SNAPSHOT-shaded.jar /usr/share/heroic/heroic.jar
COPY example/heroic-memory-example.yml /heroic.yml
COPY run-heroic.sh /usr/bin/heroic.sh

ENV JVM_DEFAULT_ARGS -Dcom.datastax.driver.FORCE_NIO=true
ENTRYPOINT ["/usr/bin/heroic.sh"]
CMD ["/heroic.yml"]
