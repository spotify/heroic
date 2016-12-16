/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.shell;

import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.serializer.SerializerFramework;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Named;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Optional;
import java.util.concurrent.Callable;

import static java.util.Optional.empty;
import static java.util.Optional.of;

@Slf4j
@RequiredArgsConstructor
@Module
public class ShellServerModule {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9190;

    final Optional<String> host;
    final Optional<Integer> port;

    @Provides
    @ShellServerScope
    @Named("shell-protocol")
    SerializerFramework serializer() {
        return ShellProtocol.setupSerializer();
    }

    @Provides
    @ShellServerScope
    Managed<ShellServerState> state(final AsyncFramework async) {
        final String host = this.host.orElse(DEFAULT_HOST);
        final int port = this.port.orElse(DEFAULT_PORT);

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
                return async.resolved();
            }
        });
    }

    @Provides
    @ShellServerScope
    @Named("shellServer")
    LifeCycle shellServerLife(LifeCycleManager manager, ShellServer shellServer) {
        return manager.build(shellServer);
    }

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Optional<String> host = empty();
        private Optional<Integer> port = empty();

        public Builder host(String host) {
            this.host = of(host);
            return this;
        }

        public Builder port(int port) {
            this.port = of(port);
            return this;
        }

        public ShellServerModule build() {
            return new ShellServerModule(host, port);
        }
    }
}
