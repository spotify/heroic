package com.spotify.heroic.shell;

import java.io.PrintWriter;
import java.util.Map;
import java.util.SortedMap;

import com.spotify.heroic.shell.ShellTasks.Task;

public interface HeroicShellBridge {
    void internalTimeoutTask(PrintWriter out, String[] args);

    void exit();

    public abstract void printTasksHelp(final PrintWriter out, final SortedMap<String, ShellTasks.Task> tasks);
}