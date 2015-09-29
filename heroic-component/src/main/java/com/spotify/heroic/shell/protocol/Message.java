package com.spotify.heroic.shell.protocol;

import eu.toolchain.serializer.AutoSerialize;
import eu.toolchain.serializer.AutoSerialize.SubType;

// @formatter:off
@AutoSerialize
@AutoSerialize.SubTypes({
    @SubType(CommandDone.class),
    @SubType(CommandOutput.class),
    @SubType(CommandsResponse.class),
    @SubType(FileNewInputStream.class),
    @SubType(FileNewOutputStream.class),
    @SubType(FileFlush.class),
    @SubType(FileClose.class),
    @SubType(FileRead.class),
    @SubType(FileWrite.class),
    @SubType(FileOpened.class),
    @SubType(FileReadResult.class),
    @SubType(EvaluateRequest.class),
    @SubType(CommandsRequest.class),
    @SubType(ErrorMessage.class),
    @SubType(Acknowledge.class),
    @SubType(Close.class)
})
// @formatter:on
public interface Message {
    <R> R visit(Visitor<R> visitor) throws Exception;

    public static interface Visitor<R> {
        R visitCommandDone(CommandDone message) throws Exception;

        R visitCommandOutput(CommandOutput message) throws Exception;

        R visitCommandsResponse(CommandsResponse message) throws Exception;

        R visitFileNewInputStream(FileNewInputStream message) throws Exception;

        R visitFileNewOutputStream(FileNewOutputStream message) throws Exception;

        R visitFileClose(FileClose message) throws Exception;

        R visitFileRead(FileRead message) throws Exception;

        R visitFileFlush(FileFlush message) throws Exception;

        R visitFileWrite(FileWrite message) throws Exception;

        R visitErrorMessage(ErrorMessage message) throws Exception;

        R visitCommandsRequest(CommandsRequest message) throws Exception;

        R visitRunTaskRequest(EvaluateRequest message) throws Exception;

        R visitFileOpened(FileOpened message) throws Exception;

        R visitFileReadResult(FileReadResult message) throws Exception;

        R visitCloseMessage(Close message) throws Exception;

        R visitOk(Acknowledge message) throws Exception;
    }
}