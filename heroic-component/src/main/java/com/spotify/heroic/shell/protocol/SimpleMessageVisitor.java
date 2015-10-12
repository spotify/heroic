package com.spotify.heroic.shell.protocol;

public abstract class SimpleMessageVisitor<R> implements Message.Visitor<R> {
    @Override
    public R visitCommandDone(CommandDone message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitCommandOutput(CommandOutput message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitCommandsResponse(CommandsResponse message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileNewInputStream(FileNewInputStream message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileNewOutputStream(FileNewOutputStream message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileClose(FileClose message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileRead(FileRead message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileFlush(FileFlush message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileWrite(FileWrite message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitCommandsRequest(CommandsRequest message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitRunTaskRequest(EvaluateRequest message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileOpened(FileOpened message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitFileReadResult(FileReadResult message) throws Exception {
        return visitUnknown(message);
    }

    @Override
    public R visitOk(Acknowledge message) throws Exception {
        return visitUnknown(message);
    }

    protected abstract R visitUnknown(Message message) throws Exception;
}