package com.spotify.heroic.shell;

import eu.toolchain.async.AsyncFuture;


public interface ShellTask {
    public TaskParameters params();

    public AsyncFuture<Void> run(final ShellIO io, TaskParameters params) throws Exception;
}