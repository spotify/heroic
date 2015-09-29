package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class FileFlush implements Message {
    final int handle;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitFileFlush(this);
    }
}