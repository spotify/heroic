package com.spotify.heroic.shell.protocol;

import java.util.List;

import lombok.Data;
import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@Data
public class RunTaskRequest implements Request {
    final List<String> command;
}