# Heroic Metrics API

Heroic is a service to make a simplified and safe API on top of metrics and
event databases.

Go to https://spotify.github.io/heroic/ for documentation.

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
