package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.ToString;

@AutoSerialize
@ToString
public class CommandDone implements Message {@Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitCommandDone(this);
    }
}