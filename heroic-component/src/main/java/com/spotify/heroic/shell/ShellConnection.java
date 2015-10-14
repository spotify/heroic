package com.spotify.heroic.shell;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.Message_Serializer;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShellConnection implements Closeable {
    final Socket socket;
    final SerialReader reader;
    final StreamSerialWriter writer;
    final Serializer<Message> message;

    public ShellConnection(final SerializerFramework framework, final Socket socket) throws IOException {
        this.socket = socket;
        this.reader = framework.readStream(socket.getInputStream());
        this.writer = framework.writeStream(socket.getOutputStream());
        this.message = new Message_Serializer(framework);
    }

    public Message receive() throws IOException {
        return message.deserialize(reader);
    }

    public void send(Message m) throws IOException {
        message.serialize(writer, m);
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    @SuppressWarnings("unchecked")
    public <T extends Message, R extends Message> R request(T request, Class<R> expected) throws IOException {
        send(request);

        final Message response = receive();

        if (!expected.isAssignableFrom(response.getClass())) {
            throw new IOException(
                    String.format("Got unexpected message (%s), expected (%s)", response.getClass(), expected));
        }

        return (R) response;
    }
}