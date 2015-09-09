package com.spotify.heroic.shell;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.shell.protocol.CommandDefinition;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;

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

    @Override
    public boolean isReady() {
        return state.isReady();
    }

    @Override
    public AsyncFuture<Void> start() {
        return state.start().lazyTransform((s) -> {
            return state.doto((state) -> {
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
            });
        });
    }

    @Override
    public AsyncFuture<Void> stop() {
        return state.stop();
    }

    public ShellServerConnection connect(ShellInterface shell) {
        return new ShellServerConnection(setupTasks(shell), async);
    }

    public List<CommandDefinition> getCommands() {
        try (Borrowed<ShellServerState> state = this.state.borrow()) {
            if (!state.isValid()) {
                throw new IllegalStateException("Server is not ready");
            }

            final List<CommandDefinition> commands = new ArrayList<>();

            for (final CoreTaskDefinition def : state.get().commands) {
                commands.add(new CommandDefinition(def.name(), def.aliases(), def.usage()));
            }

            return commands;
        }
    }

    void doRun(ShellServerState state) {
        while (true) {
            final Socket socket;

            try {
                socket = state.serverSocket.accept();
            } catch (IOException e) {
                log.info("Shutting down...", e);
                break;
            }

            final ShellServerConnection connection = connect(new ShellInterface() {
            });

            final Thread clientThread = new Thread(new ShellServerClientThread(socket, connection, state.commands));
            clientThread.setName(String.format("remote-shell-thread[%s]", socket.getRemoteSocketAddress()));
            clientThread.start();
        }
    }

    SortedMap<String, TaskDefinition> setupTasks(ShellInterface shell) {
        final SortedMap<String, TaskDefinition> tasks = new TreeMap<>();

        final TaskContext ctx = new TaskContext() {
        };

        try (final Borrowed<ShellServerState> state = this.state.borrow()) {
            if (!state.isValid()) {
                throw new IllegalStateException("Failed to borrow state");
            }

            for (Entry<String, CoreShellTaskDefinition> e : state.get().tasks.entrySet()) {
                tasks.put(e.getKey(), e.getValue().setup(shell, ctx));
            }

            return tasks;
        }
    }
}