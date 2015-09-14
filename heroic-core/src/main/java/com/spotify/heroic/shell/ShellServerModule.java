package com.spotify.heroic.shell;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.spotify.heroic.HeroicCore;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;

public class ShellServerModule extends PrivateModule {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9190;

    final String host;
    final int port;

    public ShellServerModule(@JsonProperty("host") String host, @JsonProperty("port") Integer port) {
        this.host = Optional.fromNullable(host).or(DEFAULT_HOST);
        this.port = Optional.fromNullable(port).or(DEFAULT_PORT);
    }

    @Provides
    @Singleton
    Managed<ShellServerState> state(final AsyncFramework async, final HeroicCore core) {
        return async.managed(new ManagedSetup<ShellServerState>() {
            @Override
            public AsyncFuture<ShellServerState> construct() throws Exception {
                return async.call(new Callable<ShellServerState>() {
                    @Override
                    public ShellServerState call() throws Exception {
                        final ServerSocket serverSocket = new ServerSocket();
                        serverSocket.bind(new InetSocketAddress(host, port));
                        final Collection<CoreTaskDefinition> commands = Tasks.available();
                        final SortedMap<String, CoreShellTaskDefinition> tasks = setupTasks(commands, core);
                        return new ShellServerState(serverSocket, commands, tasks);
                    }
                });
            }

            @Override
            public AsyncFuture<Void> destruct(final ShellServerState value) throws Exception {
                return async.call(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        return null;
                    };
                });
            }

            public SortedMap<String, CoreShellTaskDefinition> setupTasks(final Collection<CoreTaskDefinition> commands,
                    final HeroicCore core) throws Exception {
                final SortedMap<String, CoreShellTaskDefinition> tasks = new TreeMap<>();

                for (final CoreTaskDefinition def : commands) {
                    final CoreShellTaskDefinition instance = def.setup(core);

                    for (final String n : def.names()) {
                        tasks.put(n, instance);
                    }
                }

                return tasks;
            }
        });
    }

    @Override
    protected void configure() {
        bind(ShellServer.class);
        expose(ShellServer.class);
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private String host;
        private int port;

        public Builder host(String host) {
            this.host = checkNotNull(host, "host");
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public ShellServerModule build() {
            return new ShellServerModule(host, port);
        }
    }
}