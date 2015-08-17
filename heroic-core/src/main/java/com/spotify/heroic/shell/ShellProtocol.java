package com.spotify.heroic.shell;

import java.io.IOException;
import java.util.List;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.google.common.collect.ImmutableList;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.SerializerFramework.TypeMapping;
import eu.toolchain.serializer.TinySerializer;

@RequiredArgsConstructor
public final class ShellProtocol {
    final SerializerFramework s;

    public final Serializer<Request> request;
    public final Serializer<Response> message;

    public ShellProtocol() {
        this.s = TinySerializer.builder().build();

        final ImmutableList.Builder<TypeMapping<? extends Request, Request>> request = ImmutableList.builder();

        request.add(s.type(1, CommandsRequest.class, CommandsRequest.serializer(s)));
        request.add(s.type(2, RunTaskRequest.class, RunTaskRequest.serializer(s)));

        this.request = s.subtypes(request.build());

        final ImmutableList.Builder<TypeMapping<? extends Response, Response>> message = ImmutableList.builder();

        message.add(s.type(1, EndOfCommand.class, EndOfCommand.serializer(s)));
        message.add(s.type(2, CommandOutput.class, CommandOutput.serializer(s)));
        message.add(s.type(3, CommandsResponse.class, CommandsResponse.serializer(s)));

        this.message = s.subtypes(message.build());
    }

    public static interface Response {
    }

    public static class EndOfCommand implements Response {
        final static EndOfCommand instance = new EndOfCommand();

        static Serializer<EndOfCommand> serializer(SerializerFramework s) {
            return new Serializer<EndOfCommand>() {
                @Override
                public void serialize(SerialWriter buffer, EndOfCommand value) throws IOException {
                }

                @Override
                public EndOfCommand deserialize(SerialReader buffer) throws IOException {
                    return instance;
                }
            };
        }
    }

    @Data
    public static class CommandOutput implements Response {
        final char[] data;

        static Serializer<CommandOutput> serializer(SerializerFramework s) {
            return new Serializer<CommandOutput>() {
                final Serializer<char[]> data = s.charArray();

                @Override
                public void serialize(SerialWriter buffer, CommandOutput value) throws IOException {
                    data.serialize(buffer, value.data);
                }

                @Override
                public CommandOutput deserialize(SerialReader buffer) throws IOException {
                    final char[] data = this.data.deserialize(buffer);
                    return new CommandOutput(data);
                }
            };
        }
    }

    @Data
    public static class CommandsResponse implements Response {
        final List<CommandDefinition> commands;

        static Serializer<CommandsResponse> serializer(SerializerFramework s) {
            return new Serializer<CommandsResponse>() {
                final Serializer<List<CommandDefinition>> commands = s.list(CommandDefinition.serializer(s));

                @Override
                public void serialize(SerialWriter buffer, CommandsResponse value) throws IOException {
                    this.commands.serialize(buffer, value.commands);
                }

                @Override
                public CommandsResponse deserialize(SerialReader buffer) throws IOException {
                    final List<CommandDefinition> commands = this.commands.deserialize(buffer);
                    return new CommandsResponse(commands);
                }
            };
        }
    }

    public static interface Request {
    }

    @Data
    public static class RunTaskRequest implements Request {
        final List<String> command;

        static Serializer<RunTaskRequest> serializer(SerializerFramework s) {
            return new Serializer<RunTaskRequest>() {
                final Serializer<List<String>> command = s.list(s.string());

                @Override
                public void serialize(SerialWriter buffer, RunTaskRequest value) throws IOException {
                    this.command.serialize(buffer, value.command);
                }

                @Override
                public RunTaskRequest deserialize(SerialReader buffer) throws IOException {
                    final List<String> command = this.command.deserialize(buffer);
                    return new RunTaskRequest(command);
                }
            };
        }
    }

    public static class CommandsRequest implements Request {
        static Serializer<CommandsRequest> serializer(SerializerFramework s) {
            return new Serializer<CommandsRequest>() {
                @Override
                public void serialize(SerialWriter buffer, CommandsRequest value) throws IOException {
                }

                @Override
                public CommandsRequest deserialize(SerialReader buffer) throws IOException {
                    return new CommandsRequest();
                }
            };
        }
    }
}