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

To run the shell, first [build the project](#building), then run.

```bash
$ java -cp heroic-dist/target/heroic-dist-0.0.1.jar com.spotify.heroic.HeroicShell [config]
```

You can run individual tasks in _standalone_ mode, giving you a bit more
options (like redirecting output) like the following.

```bash
java com.spotify.heroic.HeroicShell <heroic-options> -- com.spotify.heroic.shell.task.<task-name> <task-options>
