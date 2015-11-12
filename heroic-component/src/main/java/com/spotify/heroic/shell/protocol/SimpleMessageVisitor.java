/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
