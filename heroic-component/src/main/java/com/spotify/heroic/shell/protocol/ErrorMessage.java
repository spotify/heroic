package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class ErrorMessage implements Message {
    final String message;

    @Override
    public <R> R visit(Message.Visitor<R> visitor) throws Exception {
        return visitor.visitErrorMessage(this);
    }
}