package com.spotify.heroic;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import com.spotify.heroic.shell.CoreInterface;
import com.spotify.heroic.shell.CoreShellInterface;
import com.spotify.heroic.shell.ShellInterface;
import com.spotify.heroic.shell.ShellProtocol;
import com.spotify.heroic.shell.ShellServerModule;
import com.spotify.heroic.shell.protocol.CommandDefinition;
import com.spotify.heroic.shell.protocol.CommandOutput;
import com.spotify.heroic.shell.protocol.CommandsRequest;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import com.spotify.heroic.shell.protocol.EndOfCommand;
import com.spotify.heroic.shell.protocol.Request;
import com.spotify.heroic.shell.protocol.Response;
import com.spotify.heroic.shell.protocol.RunTaskRequest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.StreamSerialWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RemoteHeroicCoreBridge implements CoreInterface {
    final InetSocketAddress address;
    final AsyncFramework async;

    public RemoteHeroicCoreBridge(InetSocketAddress address, AsyncFramework async) throws IOException {
        this.address = address;
        this.async = async;
    }

    @Override
    public CoreShellInterface setup(final ShellInterface shell) throws Exception {
        return new CoreShellInterface() {
            @Override
            public AsyncFuture<Void> command(final List<String> command, final PrintWriter out) throws Exception {
                return async.call(() -> {
                    try (final Connection c = connect()) {
                        c.send(new RunTaskRequest(command));

                        while (true) {
                            final Response response = c.receive();;

                            if (response instanceof EndOfCommand) {
                                break;
                            }

                            if (response instanceof CommandOutput) {
                                handleCommandOutput((CommandOutput) response, out);
                                continue;
                            }

                            log.error("Unhandled response: {}", response);
                        }
                    }

                    return null;
                });
            }

            private void handleCommandOutput(CommandOutput response, PrintWriter out) {
                out.write(response.getData());
                out.flush();
            }
        };
    }

    @Override
    public List<CommandDefinition> getCommands() throws Exception {
        try (final Connection c = connect()) {
            return c.request(new CommandsRequest(), CommandsResponse.class).getCommands();
        }
    }

    @Override
    public void shutdown() throws Exception {
    }

    public static RemoteHeroicCoreBridge fromConnectString(String connect, AsyncFramework async) throws IOException {
        final String host;
        final int port;

        final int index;

        if ((index = connect.indexOf(':')) > 0) {
            host = connect.substring(0, index);
            port = Integer.valueOf(connect.substring(index + 1));
        } else {
            host = connect;
            port = ShellServerModule.DEFAULT_PORT;
        }

        return fromInetSocketAddress(new InetSocketAddress(host, port), async);
    }

    private static RemoteHeroicCoreBridge fromInetSocketAddress(InetSocketAddress address, AsyncFramework async)
            throws IOException {
        return new RemoteHeroicCoreBridge(address, async);
    }

    private Connection connect() throws IOException {
        final Socket socket = new Socket();
        socket.connect(address);
        return new Connection(socket);
    }

    private static class Connection implements Closeable {
        static final ShellProtocol protocol = new ShellProtocol();

        static final Serializer<Request> requestSerializer = protocol.buildRequest();
        static final Serializer<Response> responseSerializer = protocol.buildResponse();

        private final Socket socket;
        private final StreamSerialWriter output;
        private final SerialReader input;

        public Connection(Socket socket) throws IOException {
            this.socket = socket;
            output = protocol.framework().writeStream(socket.getOutputStream());
            input = protocol.framework().readStream(socket.getInputStream());
        }

        public Response receive() throws IOException {
            return responseSerializer.deserialize(input);
        }

        public void send(Request request) throws IOException {
            requestSerializer.serialize(output, request);
            output.flush();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }

        @SuppressWarnings("unchecked")
        <R extends Request, T extends Response> T request(R request, Class<T> expected) throws IOException {
            send(request);

            final Response response = receive();

            if (!(expected.isAssignableFrom(response.getClass()))) {
                throw new IOException(
                        String.format("Got unexpected message (%s), expected (%s)", response.getClass(), expected));
            }

            return (T) response;
        }
    }
}