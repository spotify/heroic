package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class CommandOutput implements Message {
    final char[] data;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitCommandOutput(this);
    }
}