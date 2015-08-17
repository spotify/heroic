package com.spotify.heroic.shell;

import java.io.PrintWriter;

import eu.toolchain.async.AsyncFuture;


public interface ShellTask {
    public ShellTaskParams params();

    public AsyncFuture<Void> run(PrintWriter out, ShellTaskParams params) throws Exception;
}