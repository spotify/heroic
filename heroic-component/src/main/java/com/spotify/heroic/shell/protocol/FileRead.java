package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class FileRead implements Message {
    final int handle;
    final int length;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitFileRead(this);
    }
}