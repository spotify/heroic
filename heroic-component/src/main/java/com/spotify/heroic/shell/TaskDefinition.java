package com.spotify.heroic.shell;

import java.io.PrintWriter;

import eu.toolchain.async.AsyncFuture;

public interface TaskDefinition {
    TaskParameters params();

    AsyncFuture<Void> run(PrintWriter out, TaskParameters params) throws Exception;
}