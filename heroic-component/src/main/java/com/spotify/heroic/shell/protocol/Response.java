package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import eu.toolchain.serializer.AutoSerialize.SubType;


@AutoSerialize
@AutoSerialize.SubTypes({ @SubType(EndOfCommand.class), @SubType(CommandOutput.class), @SubType(CommandsResponse.class) })
public interface Response {
}