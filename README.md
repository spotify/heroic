# Heroic
[![Build Status](https://travis-ci.org/spotify/heroic.svg?branch=master)](https://travis-ci.org/spotify/heroic)

A scalable time series database based on Cassandra and Elasticsearch.
Go to https://spotify.github.io/heroic/ for documentation.

**Alpha Disclaimer:**
Heroic should in its current state be considered Alpha-grade software.

Do not use in production unless you are willing to spend a lot of time with it,
and you are OK with loosing your data to goblins.

It is currently not on a release schedule and is not versioned.

At Spotify we rely on *release branches* that we flip-flop between with Puppet
to keep us sane.

## Building

Heroic requires Java 8, and is built using maven:

```bash
$ mvn clean package
```

This will cause the `heroic-dist` module to produce a shaded jar that contains
all required dependencies.

### Building a Debian Package

This project does not provide a single debian package, this is primarily
because the current nature of the service (alpha state) does not mesh well with
stable releases.

Instead, you are encouraged to build your own using the provided scripts in
this project.

First run the `prepare-sources` script:

```bash
$ debian/bin/prepare-sources myrel 1
```

`myrel` will be the name of your release, it will be part of your package name
`debian-myrel`, it will also be suffixed to all helper tools (e.g.
`heroic-myrel`).

For the next step you'll need a Debian environment:

```bash
$ dpkg-buildpackage -uc -us
```

If you encounter problems, you can troubleshoot the build with `DH_VERBOSE`:

```bash
$ env DH_VERBOSE=1 dpkg-buildpackage -uc -us
```

## Hacking

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

[`heroic-parser`](heroic-parser) provides an Antlr4 implementation of
[`com.spotify.heroic.grammar.QueryParser`](heroic-component/src/main/java/com/spotify/heroic/grammar/QueryParser.java),
which is used to parse the Heroic DSL.

[`heroic-shell`](heroic-shell) contains
[`com.spotify.heroic.HeroicShell`](heroic-shell/src/main/java/com/spotify/heroic/HeroicShell.java),
a shell capable of either running a standalone, or connecting to an existing
Heroic instance for administration.

[`heroic-all`](heroic-all) contains dependencies and references to all modules
that makes up a Heroic distribution. This is also where profiles are defined
since they need to have access to all dependencies.

Anything in the [`repackaged`](repackaged) directory is dependencies that
include one or more Java packages that must be relocated to avoid conflicts.
These are exported under the `com.spotify.heroic.repackaged` groupId.

Finally there is [`heroic-dist`](heroic-dist), a small project that depends on
[`heroic-all`](heroic-all), [`heroic-shell`](heroic-shell), and a logging
implementation. Here is where everything is bound together into a distribution
&mdash; a shaded jar. It also provides the entry-point for services, namely
[`com.spotify.heroic.HeroicService`](heroic-dist/src/main/java/com/spotify/heroic/HeroicService.java).

### Using Eclipse

You must use at least Eclipse 4.5.0, this is due to issues with annotation
processing in earlier versions.

The following plugins are required:

* lombok (https://projectlombok.org/download.html)
* m2e-apt (install through Marketplace)
  * Enable automatic configuration of JDT APT in
    (Preferences &rarr; Maven &rarr; Annotation Processing).
* m2e checkstyle connector (will be required on initial import)
  * Configure it to use the provided [checkstyle.xml](checkstyle.xml) file in
    (Preferences &rarr; Checkstyle).
    You must set the `${basedir}` property to the path of the checked out
    heroic project.

The project contains a repackaged version of
[bigtable-client-core](http://search.maven.org/#artifactdetails%7Ccom.google.cloud.bigtable%7Cbigtable-client-core%7C0.2.1%7Cjar)
which you should install locally to make it easier for Eclipse to discover:

```bash
$ cd repackaged/bigtable && mvn clean install
```

Import the directory as a Maven project (File &rarr; Import &rarr; Maven &rarr;
Existing Maven Projects), select all modules.

## HeroicShell

Heroic comes with a shell that contains many useful tasks, these can either
be run in a readline-based shell with some basic completions and history, or
standalone.

You can use the following helper script to run the shell directly from the
project.

```bash
$ tools/heroic-shell [opts]
```

There are a few interesting options available, most notably is `--connect` that
allows the shell to connect to a remote heroic instance.

See `-h` for a full listing of options.

You can run individual tasks in _standalone_ mode, giving you a bit more
options (like redirecting output) through the following.

```bash
$ tools/heroic-shell <heroic-options> -- com.spotify.heroic.shell.task.<task-name> <task-options>
```

There are also profiles that can be activated with the `-P <profile>` switch,
available profiles are listed in `--help`.
