# DEPRECATION NOTICE

**This repo is no longer actively maintained. While it should continue to work and there are no major known bugs, we will not be improving Heroic or releasing new versions.**

# [![Heroic](/logo.42.png?raw=true "The Heroic Time Series Database")](/assets/logo_on_light.svg) Heroic

[![Build Status](https://circleci.com/gh/spotify/heroic.svg?style=svg)](https://circleci.com/gh/spotify/heroic)
[![Codecov](https://img.shields.io/codecov/c/github/spotify/heroic.svg)](https://codecov.io/gh/spotify/heroic)
[![License](https://img.shields.io/github/license/spotify/heroic.svg)](LICENSE)

A scalable time series database based on Bigtable, Cassandra, and Elasticsearch.
Go to https://spotify.github.io/heroic/ for documentation.

This project adheres to the [Open Code of Conduct](https://github.com/spotify/code-of-conduct/blob/master/code-of-conduct.md).
By participating, you are expected to honor this code.

## Install

### Docker

Docker images are available on [Docker Hub](https://hub.docker.com/r/spotify/heroic).

    $ docker run -p 8080:8080 -p 9091:9091 spotify/heroic

Heroic will now be reachable at http://localhost:8080/status.

In production it's advised to use a tagged version.


## Configuration
For help on how to write a configuration file, see the [Configuration Section][configuration] of the official documentation.

[configuration]: http://spotify.github.io/heroic/#!/docs/config

Heroic has been tested with the following services:

* Cassandra (`2.1.x`, `3.5`) when using [metric/datastax](/metric/datastax).
* [Cloud Bigtable](https://cloud.google.com/bigtable/docs/) when using
  [metric/bigtable](/metric/bigtable).
* Elasticsearch (`7.x`) when using
  [metadata/elasticsearch](/metadata/elasticsearch) or
  [suggest/elasticsearch](/suggest/elasticsearch).
* Kafka (`0.8.x`) when using [consumer/kafka](/consumer/kafka).


## Developing

### Building from source

In order to compile Heroic, you'll need:

- A Java 11 JDK
- Maven 3
- Gradle

The project is built using Gradle:

```bash
# full build, runs all tests and builds the shaded jar
./gradlew build

# only compile
./gradlew assemble

# build a single module
./gradlew heroic-metric-bigtable:build
```

The `heroic-dist` module can be used to produce a shaded jar that contains all required dependencies:

```
./gradlew heroic-dist:shadowJar
```

After building, the entry point of the service is
[`com.spotify.heroic.HeroicService`](/heroic-dist/src/main/java/com/spotify/heroic/HeroicService.java).
The following is an example of how this can be run:

```
./gradlew heroic-dist:runShadow <config>
```

which is the equivalent of doing:

```
java -jar $PWD/heroic-dist/build/libs/heroic-dist-0.0.1-SNAPSHOT-shaded.jar <config>
```


### Building with Docker

```
$ docker build -t heroic:latest .
```

This is a multi-stage build and will first build Heroic via a `./gradlew clean build` and then copy the resulting shaded jar into the runtime container.

Running heroic via docker can be done:

```
$ docker run -d -p 8080:8080 -p 9091:9091 -v /path/to/config.yml:/heroic.yml spotify/heroic:latest
```


### Logging

Logging is captured using [SLF4J](http://www.slf4j.org/), and forwarded to
[Log4j](http://logging.apache.org/log4j/).

To configure logging, define the `-Dlog4j.configurationFile=<path>`
parameter. You can use [docs/log4j2-file.xml](/docs/log4j2-file.xml) as a base.

### Testing

We run tests with Gradle:

```
# run unit tests
./gradlew test

# run integration tests
./gradlew integrationTest
```

or to run a more comprehensive set of checks:

```
./gradlew check
```

This will run:

* unit tests
* integration tests
* [Checkstyle](http://checkstyle.sourceforge.net/)
* [Coverage Reporting with Jacoco](http://eclemma.org/jacoco/)

It is strongly recommended that you run the full test suite before setting up a
pull request, otherwise it will be rejected by Travis.

#### Full Cluster Tests

Full cluster tests are defined in [heroic-dist/src/test/java](/heroic-dist/src/test/java).

This way, they have access to all the modules and parts of Heroic.

The [JVM RPC](/rpc/jvm) module is specifically designed to allow for rapid
execution of integration tests. It allows multiple cores to be defined and
communicate with each other in the same JVM instance.

* See [AbstractClusterQueryIT](/heroic-dist/src/test/java/com/spotify/heroic/AbstractClusterQueryIT.java)
* JVM-based [JvmClusterQueryIT](/heroic-dist/src/test/java/com/spotify/heroic/JvmClusterQueryIT.java)
* gRPC-based [GrpcClusterQueryIT](/heroic-dist/src/test/java/com/spotify/heroic/GrpcClusterQueryIT.java)


### Code Coverage

[![Coverage](https://codecov.io/gh/spotify/heroic/branch/master/graphs/icicle.svg)](https://codecov.io/gh/spotify/heroic/branch/master)

There's an ongoing project to improve test coverage.
Clicking the above graph will bring you to [codecov.io](https://codecov.io/gh/spotify/heroic/branches/master), where you can find areas to focus on.

#### Bypassing Validation

To bypass automatic formatting and checkstyle validation you can use the
following stanza:

```java
// @formatter:off
final List<String> list = ImmutableList.of(
   "Welcome to...",
   "... The Wild West"
);
// @formatter:on
```

To bypass a FindBugs error, you should use the `@SupressFBWarnings` annotation.

```java
@SupressFBWarnings(value="FINDBUGS_ERROR_CODE", justification="I Know Better Than FindBugs")
public class IKnowBetterThanFindbugs() {
    // ...
}
```

### Module Orientation

The Heroic project is split into a couple of modules.

The most critical one is [`heroic-component`](heroic-component). It contains
interfaces, value objects, and the basic set of dependencies necessary to glue
different components together.

Submodules include [`metric`](metric), [`suggest`](suggest),
[`metadata`](metadata), and [`aggregation`](aggregation). The first three
contain various implementations of the given backend type, while the latter
provides aggregation methods.

[`heroic-core`](heroic-core) contains the
[`com.spotify.heroic.HeroicCore`](heroic-core/src/main/java/com/spotify/heroic/HeroicCore.java)
class which is the central building block for setting up a Heroic instance.

[`heroic-elasticsearch-utils`](heroic-elasticsearch-utils) is a collection of
utilities for interacting with Elasticsearch. This is separate since we have
more than one backend that needs to talk with elasticsearch.

Finally there is [`heroic-dist`](heroic-dist), a small project that depends on all module. Here is where everything is bound together into a distribution
&mdash; a shaded jar. It also provides the entry-point for services, namely
[`com.spotify.heroic.HeroicService`](heroic-dist/src/main/java/com/spotify/heroic/HeroicService.java)
or through an interactive shell [`com.spotify.heroic.HeroicShell`](heroic-shell/src/main/java/com/spotify/heroic/HeroicShell.java).
The shell can either be run standalone or connected to an existing Heroic instance for administration.

## Contributing

Guidelines for contributing can be found [here](https://github.com/spotify/heroic/blob/master/CONTRIBUTING.md).
