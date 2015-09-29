package com.spotify.heroic.shell.protocol;

import java.util.List;

import eu.toolchain.serializer.AutoSerialize;
import lombok.Data;

@AutoSerialize
@Data
public class EvaluateRequest implements Message {
    final List<String> command;

    @Override
    public <R> R visit(Visitor<R> visitor) throws Exception {
        return visitor.visitRunTaskRequest(this);
    }
}