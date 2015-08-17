package com.spotify.heroic.shell;

import java.io.PrintWriter;
import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface CoreShellInterface {
    public AsyncFuture<Void> command(List<String> command, PrintWriter out) throws Exception;
}