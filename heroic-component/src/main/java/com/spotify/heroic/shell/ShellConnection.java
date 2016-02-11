/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.shell;

import com.spotify.heroic.shell.protocol.Message;
import com.spotify.heroic.shell.protocol.Message_Serializer;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

@Slf4j
public class ShellConnection implements Closeable {
    final Socket socket;
    final SerialReader reader;
    final StreamSerialWriter writer;
    final Serializer<Message> message;

    public ShellConnection(final SerializerFramework framework, final Socket socket)
        throws IOException {
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
    public <T extends Message, R extends Message> R request(T request, Class<R> expected)
        throws IOException {
        send(request);

        final Message response = receive();

        if (!expected.isAssignableFrom(response.getClass())) {
            throw new IOException(
                String.format("Got unexpected message (%s), expected (%s)", response.getClass(),
                    expected));
        }

        return (R) response;
    }
}
