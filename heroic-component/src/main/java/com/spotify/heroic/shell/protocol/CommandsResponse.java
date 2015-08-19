package com.spotify.heroic.shell.protocol;

import java.util.List;

import lombok.Data;
import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@Data
public class CommandsResponse implements Response {
    final List<CommandDefinition> commands;
}