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

import com.spotify.heroic.ShellTasks;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import dagger.Lazy;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.serializer.SerializerFramework;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.net.Socket;

@ToString(of = {"state"})
@Slf4j
public class ShellServer implements LifeCycles {
    private final Managed<ShellServerState> state;
    private final AsyncFramework async;
    private final SerializerFramework serializer;
    private final Lazy<ShellTasks> tasks;

    @Inject
    public ShellServer(
        Managed<ShellServerState> state, AsyncFramework async,
        @Named("shell-protocol") SerializerFramework serializer, Lazy<ShellTasks> tasks
    ) {
        this.state = state;
        this.async = async;
        this.serializer = serializer;
        this.tasks = tasks;
    }

    @Override
    public void register(final LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    AsyncFuture<Void> start() {
        return state.start().lazyTransform(s -> state.doto(state -> {
            final Thread thread = new Thread(() -> {
                try {
                    doRun(state);
                } catch (Exception e) {
                    log.error("Error in server thread", e);
                }

                log.info("Shutting down...");
            });

            thread.setName("remote-shell-server");
            thread.start();
            return async.resolved();
        }));
    }

    AsyncFuture<Void> stop() {
        return state.stop();
    }

    void doRun(ShellServerState state) {
        log.info("Running shell server...");

        while (true) {
            final Socket socket;

            try {
                socket = state.serverSocket.accept();
            } catch (IOException e) {
                log.info("Shutting down...", e);
                break;
            }

            final Runnable runnable =
                new ShellServerClientThread(socket, tasks.get(), serializer, async);
            final Thread clientThread = new Thread(runnable);
            clientThread.setName(
                String.format("remote-shell-thread[%s]", socket.getRemoteSocketAddress()));
            clientThread.start();
        }
    }
}
