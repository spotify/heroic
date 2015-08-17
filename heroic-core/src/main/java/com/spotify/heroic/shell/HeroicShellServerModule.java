package com.spotify.heroic.shell;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.spotify.heroic.shell.HeroicShellServer.State;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;

public class HeroicShellServerModule extends PrivateModule {
    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 9190;

    final String host;
    final int port;

    public HeroicShellServerModule(@JsonProperty("host") String host, @JsonProperty("port") Integer port) {
        this.host = Optional.fromNullable(host).or(DEFAULT_HOST);
        this.port = Optional.fromNullable(port).or(DEFAULT_PORT);
    }

    @Provides
    @Singleton
    Managed<HeroicShellServer.State> state(final AsyncFramework async) {
        return async.managed(new ManagedSetup<State>() {
            @Override
            public AsyncFuture<State> construct() throws Exception {
                return async.call(new Callable<State>() {
                    @Override
                    public State call() throws Exception {
                        final ServerSocket serverSocket = new ServerSocket();
                        serverSocket.bind(new InetSocketAddress(host, port));
                        return new State(serverSocket);
                    }
                });
            }

            @Override
            public AsyncFuture<Void> destruct(final State value) throws Exception {
                return async.call(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        return null;
                    };
                });
            }
        });
    }

    @Override
    protected void configure() {
        bind(HeroicShellServer.class);
        expose(HeroicShellServer.class);
    }

    public static HeroicShellServerModule defaultSupplier() {
        return new HeroicShellServerModule(null, null);
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

        public HeroicShellServerModule build() {
            return new HeroicShellServerModule(host, port);
        }
    }
}