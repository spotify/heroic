package com.spotify.heroic.shell.protocol;

import com.google.protobuf.ByteString;
import com.spotify.heroic.proto.ShellMessage.*;
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
