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

import com.spotify.heroic.proto.ShellMessage.CommandEvent;
import com.spotify.heroic.proto.ShellMessage.FileEvent;
import com.spotify.heroic.proto.ShellMessage.FileStream;
import com.spotify.heroic.proto.ShellMessage.Message;
import com.spotify.heroic.shell.protocol.SimpleMessageVisitor;
import java.io.InputStream;
import java.io.OutputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;

@Slf4j
public class ShellConnection implements Closeable {
    private final Socket socket;
    private final InputStream reader;
    private final OutputStream writer;

    public ShellConnection(final Socket socket)
        throws IOException {
        this.socket = socket;
        this.reader = socket.getInputStream();
        this.writer = socket.getOutputStream();
    }

    public Message receive() throws IOException {
        return Message.parseDelimitedFrom(reader);
    }

    public void send(Message m) throws IOException {
        m.writeDelimitedTo(writer);
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    public <R> R receiveAndParse(SimpleMessageVisitor<R> visitor) throws Exception {
        final Message message  = receive();

        switch (message.getTypeCase()) {
            case ACKNOWLEDGE:
                return visitor.visitOk();
            case COMMAND_EVENT:
                final CommandEvent commandEvent = message.getCommandEvent();
                switch (commandEvent.getEvent()) {
                    case DONE: return visitor.visitCommandDone(commandEvent);
                    case OUTPUT: return visitor.visitCommandOutput(commandEvent);
                    case FLUSH: return visitor.visitCommandOutputFlush(commandEvent);
                    case REQUEST: return visitor.visitCommandsRequest(commandEvent);
                    default: return visitor.visitUnknown(message);
                }
            case COMMANDS_RESPONSE:
                return visitor.visitCommandsResponse(message.getCommandsResponse());
            case EVALUATE_REQUEST:
                return visitor.visitRunTaskRequest(message.getEvaluateRequest());
            case FILE_EVENT:
                final FileEvent fileEvent = message.getFileEvent();
                switch (fileEvent.getEvent()) {
                    case CLOSE: return visitor.visitFileClose(fileEvent);
                    case FLUSH: return visitor.visitFileFlush(fileEvent);
                    case OPENED: return visitor.visitFileOpened(fileEvent);
                    default: return visitor.visitUnknown(message);
                }
            case FILE_STREAM:
                final FileStream fileStream = message.getFileStream();
                switch (fileStream.getType()) {
                    case INPUT: return visitor.visitFileNewInputStream(fileStream);
                    case OUTPUT: return visitor.visitFileNewOutputStream(fileStream);
                    default: return visitor.visitUnknown(message);
                }
            case FILE_READ:
                return visitor.visitFileRead(message.getFileRead());
            case FILE_READ_RESULT:
                return visitor.visitFileReadResult(message.getFileReadResult());
            case FILE_WRITE:
                return visitor.visitFileWrite(message.getFileWrite());
            default:
                return visitor.visitUnknown(message);
        }
    }

    public Message request(Message request) throws IOException {
        send(request);
        return receive();
    }

    /*
     * Send a message on the connection that expects an Acknowledgement to be sent back.
     */
    public void ackedRequest(Message message) throws IOException {
        final Message response = request(message);
        if (!response.hasAcknowledge()) {
            throw new IOException("Got unexpected message closing file");
        }
    }
}
