package com.spotify.heroic.shell.protocol;

import lombok.Data;
import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@Data
public class CommandOutput implements Response {
    final char[] data;
}