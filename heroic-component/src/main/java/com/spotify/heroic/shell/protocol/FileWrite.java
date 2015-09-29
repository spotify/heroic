package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class FileWrite implements Message {
    final int handle;
    final byte[] data;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitFileWrite(this);
    }
}