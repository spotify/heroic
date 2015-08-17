package com.spotify.heroic.shell;

import java.io.IOException;
import java.util.List;

import lombok.Data;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;


@Data
public class CommandDefinition {
    final String name;
    final List<String> aliases;
    final String usage;

    static Serializer<CommandDefinition> serializer(SerializerFramework s) {
        return new Serializer<CommandDefinition>() {
            final Serializer<String> string = s.string();
            final Serializer<List<String>> aliases = s.list(s.string());

            @Override
            public void serialize(SerialWriter buffer, CommandDefinition value) throws IOException {
                this.string.serialize(buffer, value.name);
                this.aliases.serialize(buffer, value.aliases);
                this.string.serialize(buffer, value.usage);
            }

            @Override
            public CommandDefinition deserialize(SerialReader buffer) throws IOException {
                final String name = this.string.deserialize(buffer);
                final List<String> aliases = this.aliases.deserialize(buffer);
                final String usage = this.string.deserialize(buffer);
                return new CommandDefinition(name, aliases, usage);
            }
        };
    }
}