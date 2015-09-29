package com.spotify.heroic.shell;

import java.io.Closeable;
import java.io.IOException;

import com.spotify.heroic.shell.protocol.Close;
import com.spotify.heroic.shell.protocol.ErrorMessage;
import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.Message_Serializer;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;

public class ShellConnection implements Closeable {
    final SerialReader reader;
    final StreamSerialWriter writer;
    final Serializer<Message> message;

    public ShellConnection(final SerializerFramework framework, final SerialReader reader, final StreamSerialWriter writer) throws IOException {
        this.reader = reader;
        this.writer = writer;
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
        send(new Close());
        writer.close();
    }

    @SuppressWarnings("unchecked")
    public <T extends Message, R extends Message> R request(T request, Class<R> expected) throws IOException {
        send(request);

        final Message response = receive();

        if (response instanceof ErrorMessage) {
            throw new IOException("Remote error: " + (ErrorMessage)response);
        }

        if (!expected.isAssignableFrom(response.getClass())) {
            throw new IOException(
                    String.format("Got unexpected message (%s), expected (%s)", response.getClass(), expected));
        }

        return (R) response;
    }
}