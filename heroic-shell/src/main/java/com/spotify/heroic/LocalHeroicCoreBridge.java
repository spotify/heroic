package com.spotify.heroic;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.Callable;

import lombok.RequiredArgsConstructor;

import com.google.inject.Inject;
import com.spotify.heroic.shell.CoreInterface;
import com.spotify.heroic.shell.CoreShellInterface;
import com.spotify.heroic.shell.ShellInterface;
import com.spotify.heroic.shell.ShellServer;
import com.spotify.heroic.shell.ShellServerConnection;
import com.spotify.heroic.shell.protocol.CommandDefinition;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@RequiredArgsConstructor
public class LocalHeroicCoreBridge implements CoreInterface {
    final HeroicCore core;
    final AsyncFramework async;

    @Override
    public CoreShellInterface setup(final ShellInterface shell) throws Exception {
        final ShellServerConnection connection = core.inject(new Callable<ShellServerConnection>() {
            @Inject
            ShellServer server;

            @Override
            public ShellServerConnection call() throws Exception {
                return server.connect(shell);
            }
        }).call();

        return new CoreShellInterface() {
            @Override
            public AsyncFuture<Void> command(List<String> command, PrintWriter out) throws Exception {
                return connection.runTask(command, out);
            }
        };
    }

    @Override
    public List<CommandDefinition> getCommands() throws Exception {
        return core.inject(new Callable<List<CommandDefinition>>() {
            @Inject
            ShellServer server;

            @Override
            public List<CommandDefinition> call() throws Exception {
                return server.getCommands();
            }
        }).call();
    }

    @Override
    public void shutdown() throws Exception {
        core.shutdown();
    }
}