package com.spotify.heroic.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.LifeCycle;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;

@RequiredArgsConstructor
@ToString
@Slf4j
public class HeroicShellServer implements LifeCycle {
    @Inject
    Managed<State> state;

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
    public AsyncFuture<Void> start() throws Exception {
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

    void doRun(State state) {
        while (true) {
            final Socket socket;

            try {
                socket = state.serverSocket.accept();
            } catch (IOException e) {
                log.info("Shutting down...", e);
                break;
            }

            final Thread clientThread = new Thread(new ClientThread(socket));
            clientThread.setName(String.format("remote-shell-thread[%s]", socket.getRemoteSocketAddress()));
            clientThread.start();
        }
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return state.stop();
    }

    @RequiredArgsConstructor
    class ClientThread implements Runnable {
        final Socket socket;

        @Override
        public void run() {
            log.info("Connected");

            try {
                doRun();
            } catch (Exception e) {
                log.error("Exception thrown in client thread");
            }

            try {
                socket.close();
            } catch (IOException e) {
                log.error("Failed to close client socket", e);
            }

            log.info("Shutting down");
        }

        void doRun() throws Exception {
            try (final InputStream input = socket.getInputStream()) {
                try (final PrintWriter output = new PrintWriter(socket.getOutputStream())) {
                    output.println("Hello\n");
                    output.flush();
                }
            }
        }
    }

    @RequiredArgsConstructor
    static class State {
        final ServerSocket serverSocket;
    }
}