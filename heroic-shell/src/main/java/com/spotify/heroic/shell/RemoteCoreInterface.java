package com.spotify.heroic.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.spotify.heroic.shell.protocol.Acknowledge;
import com.spotify.heroic.shell.protocol.CommandDefinition;
import com.spotify.heroic.shell.protocol.CommandDone;
import com.spotify.heroic.shell.protocol.CommandOutput;
import com.spotify.heroic.shell.protocol.CommandsRequest;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import com.spotify.heroic.shell.protocol.ErrorMessage;
import com.spotify.heroic.shell.protocol.EvaluateRequest;
import com.spotify.heroic.shell.protocol.FileClose;
import com.spotify.heroic.shell.protocol.FileFlush;
import com.spotify.heroic.shell.protocol.FileNewInputStream;
import com.spotify.heroic.shell.protocol.FileNewOutputStream;
import com.spotify.heroic.shell.protocol.FileOpened;
import com.spotify.heroic.shell.protocol.FileRead;
import com.spotify.heroic.shell.protocol.FileReadResult;
import com.spotify.heroic.shell.protocol.FileWrite;
import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteCoreInterface implements CoreInterface {
    public static final int DEFAULT_PORT = 9190;

    final InetSocketAddress address;
    final AsyncFramework async;
    final SerializerFramework serializer;

    public RemoteCoreInterface(InetSocketAddress address, AsyncFramework async, SerializerFramework serializer) throws IOException {
        this.address = address;
        this.async = async;
        this.serializer = serializer;
    }

    @Override
    public AsyncFuture<Void> evaluate(final List<String> command, final ShellIO io) throws Exception {
        return async.call(() -> {
            try (final ShellConnection c = connect()) {
                c.send(new EvaluateRequest(command));

                final AtomicBoolean running = new AtomicBoolean(true);

                final Message.Visitor<Message> visitor = setupVisitor(io, c, running);

                while (running.get()) {
                    final Message in = c.receive();
                    final Message out;

                    try {
                        out = in.visit(visitor);
                    } catch (final Exception e) {
                        log.error("Command failed", e);
                        c.send(new ErrorMessage(e.toString()));
                        continue;
                    }

                    c.send(out);
                }
            }

            return null;
        });
    }

    @Override
    public List<CommandDefinition> commands() throws Exception {
        try (final ShellConnection c = connect()) {
            return c.request(new CommandsRequest(), CommandsResponse.class).getCommands();
        }
    }

    @Override
    public void shutdown() throws Exception {
    }

    public static RemoteCoreInterface fromConnectString(String connect, AsyncFramework async,
            SerializerFramework serializer) throws IOException {
        final String host;
        final int port;

        final int index;

        if ((index = connect.indexOf(':')) > 0) {
            host = connect.substring(0, index);
            port = Integer.valueOf(connect.substring(index + 1));
        } else {
            host = connect;
            port = DEFAULT_PORT;
        }

        return new RemoteCoreInterface(new InetSocketAddress(host, port), async, serializer);
    }

    private Message.Visitor<Message> setupVisitor(final ShellIO io,
            final ShellConnection c, final AtomicBoolean running) {
        return new SimpleMessageVisitor<Message>() {
            final AtomicInteger handle = new AtomicInteger();

            final Map<Integer, InputStream> reading = new HashMap<>();
            final Map<Integer, OutputStream> writing = new HashMap<>();
            final Map<Integer, Callable<Void>> closers = new HashMap<>();

            public Message visitCommandDone(CommandDone m) {
                running.set(false);
                return new Acknowledge();
            }

            @Override
            public Message visitCommandOutput(CommandOutput m) {
                io.out().write(m.getData());
                io.out().flush();
                return new Acknowledge();
            }

            @Override
            public Message visitFileNewInputStream(FileNewInputStream m) throws Exception {
                final InputStream in = io.newInputStream(Paths.get(m.getPath()), m.getOptionsAsArray());

                final int handle = this.handle.incrementAndGet();

                reading.put(handle, in);
                closers.put(handle, new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        in.close();
                        reading.remove(handle);
                        return null;
                    }
                });

                return new FileOpened(handle);
            }

            @Override
            public Message visitFileNewOutputStream(FileNewOutputStream m) throws Exception {
                final OutputStream out = io.newOutputStream(Paths.get(m.getPath()), m.getOptionsAsArray());

                final int handle = this.handle.incrementAndGet();

                writing.put(handle, out);
                closers.put(handle, new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        out.close();
                        writing.remove(handle);
                        return null;
                    }
                });

                return new FileOpened(handle);
            }

            @Override
            public Message visitFileFlush(FileFlush m) throws Exception {
                writer(m.getHandle()).flush();
                return new Acknowledge();
            }

            @Override
            public Message visitFileClose(FileClose m) throws Exception {
                closer(m.getHandle()).call();
                return new Acknowledge();
            }

            @Override
            public Message visitFileWrite(FileWrite m) throws Exception {
                final byte[] data = m.getData();
                writer(m.getHandle()).write(data, 0, data.length);
                return new Acknowledge();
            }

            @Override
            public Message visitFileRead(FileRead m) throws Exception {
                final byte[] buffer = new byte[m.getLength()];
                reader(m.getHandle()).read(buffer, 0, m.getLength());
                return new FileReadResult(buffer);
            }

            @Override
            protected Message visitUnknown(Message message) {
                throw new IllegalArgumentException("Unhandled message: " + message);
            }

            private InputStream reader(int handle) throws Exception {
                final InputStream r = reading.get(handle);

                if (r == null) {
                    throw new Exception("No such handle: " + handle);
                }

                return r;
            }

            private OutputStream writer(int handle) throws Exception {
                final OutputStream w = writing.get(handle);

                if (w == null) {
                    throw new Exception("No such handle: " + handle);
                }

                return w;
            }

            private Callable<Void> closer(int handle) throws Exception {
                final Callable<Void> closer = closers.get(handle);

                if (closer == null) {
                    throw new Exception("No such handle: " + handle);
                }

                return closer;
            }
        };
    }

    private ShellConnection connect() throws IOException {
        final Socket socket = new Socket();

        socket.connect(address);

        final SerialReader reader = serializer.readStream(socket.getInputStream());
        final StreamSerialWriter writer = serializer.writeStream(socket.getOutputStream());

        return new ShellConnection(serializer, reader, writer) {
            @Override
            public void close() throws IOException {
                try {
                    super.close();
                } finally {
                    socket.close();
                }
            }
        };
    }
}