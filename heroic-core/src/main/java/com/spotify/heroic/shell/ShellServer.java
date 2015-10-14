package com.spotify.heroic.shell;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.shell.protocol.CommandDefinition;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
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

    @Override
    public boolean isReady() {
        return state.isReady();
    }

    @Override
    public AsyncFuture<Void> start() {
        return state.start().lazyTransform(s ->
            state.doto(state -> {
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
            })
        );
    }

    @Override
    public AsyncFuture<Void> stop() {
        return state.stop();
    }

    public List<CommandDefinition> commands() {
        try (Borrowed<ShellServerState> state = this.state.borrow()) {
            if (!state.isValid()) {
                throw new IllegalStateException("Server is not ready");
            }

            final List<CommandDefinition> commands = new ArrayList<>();

            for (final ShellTaskDefinition def : state.get().commands) {
                commands.add(new CommandDefinition(def.name(), def.aliases(), def.usage()));
            }

            return commands;
        }
    }

    public ShellTasks tasks() {
        try (final Borrowed<ShellServerState> s = state.borrow()) {
            if (!s.isValid()) {
                throw new IllegalStateException("Failed to borrow state");
            }

            return s.get().tasks;
        }
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

            final ShellTasks tasks = tasks();
            final Runnable runnable = new ShellServerClientThread(socket, tasks, state.commands, serializer, async);
            final Thread clientThread = new Thread(runnable);
            clientThread.setName(String.format("remote-shell-thread[%s]", socket.getRemoteSocketAddress()));
            clientThread.start();
        }
    }
}