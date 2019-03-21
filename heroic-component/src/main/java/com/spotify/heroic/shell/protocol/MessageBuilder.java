/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.shell.protocol;

import com.google.protobuf.ByteString;
import com.spotify.heroic.proto.ShellMessage.CommandEvent;
import com.spotify.heroic.proto.ShellMessage.CommandsResponse;
import com.spotify.heroic.proto.ShellMessage.FileEvent;
import com.spotify.heroic.proto.ShellMessage.FileStream;
import com.spotify.heroic.proto.ShellMessage.Message;
import java.util.List;

public class MessageBuilder {
    public static Message acknowledge() {
        final Message.Builder builder = Message.newBuilder();
        builder.getAcknowledgeBuilder();
        return builder.build();
    }

    public static Message evaluateRequest(final List<String> commands) {
        final Message.Builder builder = Message.newBuilder();
        builder.getEvaluateRequestBuilder().addAllCommand(commands);
        return builder.build();
    }

    public static Message commandEvent(CommandEvent.Event event) {
        final Message.Builder builder = Message.newBuilder();
        builder.getCommandEventBuilder().setEvent(event);
        return builder.build();
    }

    public static Message commandEvent(CommandEvent.Event event, String data) {
        final Message.Builder builder = Message.newBuilder();
        builder.getCommandEventBuilder().setEvent(event).setData(data);
        return builder.build();
    }

    public static Message commandsResponse(Iterable<CommandsResponse.CommandDefinition> commands) {
        final Message.Builder builder = Message.newBuilder();
        builder.getCommandsResponseBuilder().addAllCommands(commands);
        return builder.build();
    }

    public static Message fileEvent(Integer handle, FileEvent.Event event) {
        final Message.Builder builder = Message.newBuilder();
        builder.getFileEventBuilder().setHandle(handle).setEvent(event);
        return builder.build();
    }

    public static Message fileRead(int handle, int length) {
        final Message.Builder builder = Message.newBuilder();
        builder.getFileReadBuilder().setHandle(handle).setLength(length);
        return builder.build();
    }

    public static Message fileReadResult(ByteString bytes) {
        final Message.Builder builder = Message.newBuilder();
        builder.getFileReadResultBuilder().setDataBytes(bytes);
        return builder.build();
    }

    public static Message fileStream(String path, FileStream.Type type) {
        final Message.Builder builder = Message.newBuilder();
        builder.getFileStreamBuilder().setPath(path).setType(type);
        return builder.build();
    }

    public static Message fileWrite(int handle, ByteString data) {
        final Message.Builder builder = Message.newBuilder();
        builder.getFileWriteBuilder().setHandle(handle).setDataBytes(data);
        return builder.build();
    }
}
