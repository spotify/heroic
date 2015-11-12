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

import java.io.IOException;
import java.net.Socket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.HeroicShellTasks;
import com.spotify.heroic.common.LifeCycle;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@ToString(of = { "state" })
@Slf4j
public class ShellServer implements LifeCycle {
    @Inject
    Managed<ShellServerState> state;

    @Inject
    AsyncFramework async;

    @Inject
    @Named("application/json")
    ObjectMapper mapper;

    @Inject
    @Named("shell-protocol")
    SerializerFramework serializer;

    @Inject
    HeroicShellTasks tasks;

    @Override
    public boolean isReady() {
        return state.isReady();
    }

    @Override
    public AsyncFuture<Void> start() {
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

    @Override
    public AsyncFuture<Void> stop() {
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

            final Runnable runnable = new ShellServerClientThread(socket, tasks, serializer, async);
            final Thread clientThread = new Thread(runnable);
            clientThread.setName(
                    String.format("remote-shell-thread[%s]", socket.getRemoteSocketAddress()));
            clientThread.start();
        }
    }
}
