package com.spotify.heroic.shell.protocol;

import java.util.List;

import lombok.Data;
import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@Data
public class CommandDefinition {
    final String name;
    final List<String> aliases;
    final String usage;
}