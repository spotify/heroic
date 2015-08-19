package com.spotify.heroic.shell.protocol;

import lombok.ToString;
import eu.toolchain.serializer.AutoSerialize;

@AutoSerialize
@ToString
public class EndOfCommand implements Response {
}