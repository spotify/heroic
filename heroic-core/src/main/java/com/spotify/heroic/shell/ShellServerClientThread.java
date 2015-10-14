package com.spotify.heroic.shell;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.spotify.heroic.shell.protocol.CommandDefinition;
import com.spotify.heroic.shell.protocol.CommandDone;
import com.spotify.heroic.shell.protocol.CommandsRequest;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import com.spotify.heroic.shell.protocol.EvaluateRequest;
import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class ShellServerClientThread implements Runnable {
    final Socket socket;
    final ShellTasks tasks;
    final Collection<ShellTaskDefinition> available;
    final SerializerFramework serializer;
    final AsyncFramework async;

    @Override
    public void run() {
        try {
            doRun();
        } catch (Exception e) {
            log.error("Exception thrown in client thread", e);
        }

        try {
            socket.close();
        } catch (IOException e) {
            log.error("Failed to close client socket", e);
        }
    }

    void doRun() throws Exception {
        try (final ServerConnection ch = new ServerConnection(serializer, socket)) {
            final SimpleMessageVisitor<AsyncFuture<Void>> visitor = new SimpleMessageVisitor<AsyncFuture<Void>>() {
                @Override
                public AsyncFuture<Void> visitCommandsRequest(CommandsRequest message) throws Exception {
                    final List<CommandDefinition> commands = new ArrayList<>();

                    for (final ShellTaskDefinition def : available) {
                        commands.add(new CommandDefinition(def.name(), def.aliases(), def.usage()));
                    }

                    ch.send(new CommandsResponse(commands));
                    return async.resolved();
                }

                @Override
                public AsyncFuture<Void> visitRunTaskRequest(EvaluateRequest message) throws Exception {
                    log.info("Run task: {}", message);

                    return tasks.evaluate(message.getCommand(), ch);
                }

                @Override
                protected AsyncFuture<Void> visitUnknown(Message message) throws Exception {
                    return async.failed(new RuntimeException("Unhandled message: " + message));
                }
            };

            final AsyncFuture<Void> future = ch.receive().visit(visitor);

            try {
                future.get();
            } catch(Exception e) {
                log.error("Command Failed", e);
                ch.out().println("Command Failed: " + e.getMessage());
                e.printStackTrace(ch.out());
                ch.out().flush();
            }

            ch.send(new CommandDone());
        }
    }
}