# Heroic Metrics API

Heroic a massively scalable TSDB based on Cassandra and Elasticsearch.

Go to https://spotify.github.io/heroic/ for documentation.

**Alpha Disclaimer:**
Heroic should in its current state be considered Alpha-grade software.

Do not use in production unless you are willing to spend a lot of time with it, and you are OK with loosing your data to goblins.

It is currently not on a release schedule and is not versioned.

At Spotify we rely on *release branches* that we flip-flop between with Puppet to keep us sane.

## Building

Heroic is built using maven, like the following example:

```bash
$ mvn clean package
```

This will cause the internal-dist to produce a shaded jar that contains all
required dependencies.

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
