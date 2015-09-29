package com.spotify.heroic.shell.protocol;

import java.util.List;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class CommandsResponse implements Message {
    final List<CommandDefinition> commands;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitCommandsResponse(this);
    }
}