package com.spotify.heroic.shell;

public interface CoreShellTaskDefinition {
    TaskDefinition setup(ShellInterface shell, TaskContext ctx);
}