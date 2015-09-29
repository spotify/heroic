package com.spotify.heroic.shell.protocol;

import java.nio.file.StandardOpenOption;
import java.util.List;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class FileNewInputStream implements Message {
    final String path;
    final List<StandardOpenOption> options;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitFileNewInputStream(this);
    }

    public StandardOpenOption[] getOptionsAsArray() {
        return options.toArray(new StandardOpenOption[0]);
    }
}