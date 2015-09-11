package com.spotify.heroic.shell;

import java.io.CharArrayWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.spotify.heroic.shell.protocol.CommandDefinition;
import com.spotify.heroic.shell.protocol.CommandOutput;
import com.spotify.heroic.shell.protocol.CommandsRequest;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import com.spotify.heroic.shell.protocol.EndOfCommand;
import com.spotify.heroic.shell.protocol.Request;
import com.spotify.heroic.shell.protocol.Response;
import com.spotify.heroic.shell.protocol.RunTaskRequest;

import eu.toolchain.async.FutureDone;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.StreamSerialWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
class ShellServerClientThread implements Runnable {
    final Socket socket;
    final ShellServerConnection connection;
    final Collection<CoreTaskDefinition> commands;

    final ShellProtocol protocol = new ShellProtocol();

    final Serializer<Request> requestSerializer = protocol.buildRequest();
    final Serializer<Response> responseSerializer = protocol.buildResponse();

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
            final SerialReader reader = protocol.framework().readStream(input);

            try (final OutputStream output = socket.getOutputStream()) {
                final StreamSerialWriter writer = protocol.framework().writeStream(output);
                final PrintWriter out = setupPrintWriter(writer);

                while (true) {
                    final Request request;

                    try {
                        request = requestSerializer.deserialize(reader);
                        handleRequest(writer, request, out);
                        output.flush();
                    } catch (EOFException e) {
                        log.info("closed connection");
                        break;
                    }
                }
            }
        }
    }

    PrintWriter setupPrintWriter(final StreamSerialWriter writer) {
        return new PrintWriter(new Writer() {
            CharArrayWriter array = new CharArrayWriter();

            @Override
            public void close() throws IOException {
            }

            @Override
            public void flush() throws IOException {
                final char[] data = array.toCharArray();

                responseSerializer.serialize(writer, new CommandOutput(data));
                writer.flush();

                array = new CharArrayWriter();
            }

            @Override
            public void write(char[] cbuf, int off, int len) throws IOException {
                array.write(cbuf, off, len);
            }
        });
    }

    void handleRequest(final StreamSerialWriter writer, Request request, PrintWriter out) throws Exception {
        if (request instanceof CommandsRequest) {
            handleCommandsRequest(writer, (CommandsRequest) request);
            return;
        }

        if (request instanceof RunTaskRequest) {
            handleRunTask(writer, (RunTaskRequest) request, out);
            return;
        }

        log.error("Unexpected message {}, closing connection", request);
        socket.close();
    }

    void handleCommandsRequest(final StreamSerialWriter writer, final CommandsRequest request) throws IOException {
        final List<CommandDefinition> commands = new ArrayList<>();

        for (final CoreTaskDefinition def : this.commands) {
            commands.add(new CommandDefinition(def.name(), def.aliases(), def.usage()));
        }

        responseSerializer.serialize(writer, new CommandsResponse(commands));
        writer.flush();
    }

    void handleRunTask(final StreamSerialWriter writer, final RunTaskRequest request, final PrintWriter out) throws Exception {
        log.info("Run task: {}", request);

        connection.runTask(request.getCommand(), out).on(new FutureDone<Void>() {
            @Override
            public void failed(Throwable cause) throws Exception {
                out.println("Command failed");
                cause.printStackTrace(out);
                out.flush();
            }

            @Override
            public void resolved(Void result) throws Exception {
                out.flush();
            }

            @Override
            public void cancelled() throws Exception {
                out.println("Command cancelled");
                out.flush();
            }
        }).on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                responseSerializer.serialize(writer, new EndOfCommand());
                writer.flush();
            }
        });
    }
}