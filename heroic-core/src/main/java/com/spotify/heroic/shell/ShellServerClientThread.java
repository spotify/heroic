package com.spotify.heroic.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

import eu.toolchain.async.FutureDone;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class ShellServerClientThread implements Runnable {
    final Socket socket;
    final ShellTasks tasks;
    final Collection<ShellTaskDefinition> available;
    final SerializerFramework serializer;

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
        try (final InputStream input = socket.getInputStream()) {
            final SerialReader reader = serializer.readStream(input);

            try (final OutputStream output = socket.getOutputStream()) {
                final StreamSerialWriter writer = serializer.writeStream(output);

                try (final ServerConnection ch = new ServerConnection(serializer, reader, writer)) {
                    ch.receive().visit(new SimpleMessageVisitor<Void>() {
                        @Override
                        public Void visitCommandsRequest(CommandsRequest message) throws Exception {
                            final List<CommandDefinition> commands = new ArrayList<>();

                            for (final ShellTaskDefinition def : available) {
                                commands.add(new CommandDefinition(def.name(), def.aliases(), def.usage()));
                            }

                            ch.send(new CommandsResponse(commands));
                            return null;
                        }

                        @Override
                        public Void visitRunTaskRequest(EvaluateRequest message) throws Exception {
                            log.info("Run task: {}", message);

                            tasks.evaluate(message.getCommand(), ch).onDone(new FutureDone<Void>() {
                                @Override
                                public void failed(Throwable cause) throws Exception {
                                    log.error("Command Failed", cause);
                                    ch.out().println("Command Failed: " + cause.getMessage());
                                    ch.out().flush();
                                    ch.send(new CommandDone());
                                }

                                @Override
                                public void resolved(Void result) throws Exception {
                                    ch.send(new CommandDone());
                                }

                                @Override
                                public void cancelled() throws Exception {
                                    ch.out().println("Command cancelled");
                                    ch.out().flush();
                                    ch.send(new CommandDone());
                                }
                            });

                            return null;
                        }

                        @Override
                        protected Void visitUnknown(Message message) throws Exception {
                            throw new RuntimeException("Unhandled message: " + message);
                        }
                    });
                }
            }
        }
    }
}