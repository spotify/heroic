# Heroic Metrics API

Heroic is a service to make a simplified and safe API on top of metrics and
event databases.

Go to https://spotify.github.io/heroic/ for documentation.

**Alpha Disclaimer:**
Heroic should in its current state be considered Alpha-grade software.

Do not use in production unless you are willing to spend a lot of time with it, and you are OK with loosing your data to goblins.

It is currently not on a release schedule and is not versioned.

At Spotify we rely on *release branches* that we flip-flop between with Puppet to keep us sane.

## Building

Heroic is built using maven, like the following example.

```bash
$ mvn clean package
```

This will cause the internal-dist to produce a shaded jar that contains all
required dependencies to operate the service.

## HeroicShell

Heroic comes with a shell that contains a set of useful tasks, these can either
be run in a readline-based shell with some basic completions and history, or
standalone.

You can use the following helper script to run the shell.

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

## Profiles

Profiles are small configuration units which can be merged into the overall
configuration.

They are activated with the `-P <profile>` switch, available profiles are
listed in `--help`.

#### Examples

Start a kafka consumer that writes data into memory:

```bash
tools/heroic-shell \
    -P kafka-consumer -X kafka.zookeeper=<zookeeper> -X kafka.topics=<topic1>,<topic2> -X kafka.schema=com.spotify.heroic.consumer.schemas.Spotify100\
    -P memory
```
