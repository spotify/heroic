package com.spotify.heroic.shell;

import static com.spotify.heroic.common.Optionals.pickOptional;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.serializer.SerializerFramework;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class ShellServerModule extends PrivateModule {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9190;

    final String host;
    final int port;

    @Provides
    @Singleton
    @Named("shell-protocol")
    SerializerFramework serializer() {
        return ShellProtocol.setupSerializer();
    }

    @Provides
    @Singleton
    Managed<ShellServerState> state(final AsyncFramework async) {
        return async.managed(new ManagedSetup<ShellServerState>() {
            @Override
            public AsyncFuture<ShellServerState> construct() throws Exception {
                return async.call(new Callable<ShellServerState>() {
                    @Override
                    public ShellServerState call() throws Exception {
                        log.info("Binding to {}:{}", host, port);
                        final ServerSocket serverSocket = new ServerSocket();
                        serverSocket.bind(new InetSocketAddress(host, port));
                        return new ShellServerState(serverSocket);
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

    @NoArgsConstructor(access=AccessLevel.PRIVATE)
    @AllArgsConstructor(access=AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<String> host = empty();
        private Optional<Integer> port = empty();

        public Builder(@JsonProperty("host") String host, @JsonProperty("port") Integer port) {
            this.host = ofNullable(host);
            this.port = ofNullable(port);
        }

        public Builder host(String host) {
            this.host = of(host);
            return this;
        }

        public Builder port(int port) {
            this.port = of(port);
            return this;
        }

        public Builder merge(Builder o) {
            return new Builder(pickOptional(host, o.host), pickOptional(port, o.port));
        }

        public ShellServerModule build() {
            return new ShellServerModule(host.orElse(DEFAULT_HOST), port.orElse(DEFAULT_PORT));
        }
    }
}