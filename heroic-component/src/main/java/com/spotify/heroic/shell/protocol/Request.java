package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import eu.toolchain.serializer.AutoSerialize.SubType;

@AutoSerialize
@AutoSerialize.SubTypes({ @SubType(RunTaskRequest.class), @SubType(CommandsRequest.class) })
public interface Request {
}