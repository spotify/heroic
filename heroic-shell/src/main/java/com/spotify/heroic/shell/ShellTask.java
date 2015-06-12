package com.spotify.heroic.shell;

import java.io.PrintWriter;

import com.spotify.heroic.HeroicCore;

import eu.toolchain.async.AsyncFuture;


public interface ShellTask {
    public ShellTaskParams params();

    public void standaloneConfig(HeroicCore.Builder builder, ShellTaskParams params);

    public AsyncFuture<Void> run(PrintWriter out, ShellTaskParams params) throws Exception;
}