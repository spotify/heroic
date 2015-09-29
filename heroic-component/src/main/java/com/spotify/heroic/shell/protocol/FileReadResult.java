package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class FileReadResult implements Message {
    final byte[] data;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitFileReadResult(this);
    }
}