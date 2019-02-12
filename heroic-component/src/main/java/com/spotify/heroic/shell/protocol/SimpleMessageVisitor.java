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

import com.spotify.heroic.proto.ShellMessage.*;

public abstract class SimpleMessageVisitor<R> {
    public R visitCommandDone(CommandEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitCommandOutput(CommandEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitCommandOutputFlush(CommandEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitCommandsResponse(CommandsResponse message) throws Exception {
        return visitUnknown();
    }

    public R visitFileNewInputStream(FileStream message) throws Exception {
        return visitUnknown();
    }

    public R visitFileNewOutputStream(FileStream message) throws Exception {
        return visitUnknown();
    }

    public R visitFileClose(FileEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitFileRead(FileRead message) throws Exception {
        return visitUnknown();
    }

    public R visitFileFlush(FileEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitFileWrite(FileWrite message) throws Exception {
        return visitUnknown();
    }

    public R visitCommandsRequest(CommandEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitRunTaskRequest(EvaluateRequest message) throws Exception {
        return visitUnknown();
    }

    public R visitFileOpened(FileEvent message) throws Exception {
        return visitUnknown();
    }

    public R visitFileReadResult(FileReadResult message) throws Exception {
        return visitUnknown();
    }

    public R visitOk() throws Exception {
        return visitUnknown();
    }

    public R visitUnknown() throws Exception {
        throw new IllegalArgumentException("Unhandled message");
    }

    public R visitUnknown(Message message) throws Exception {
        throw new IllegalArgumentException("Unhandled message: " + message.toString());
    }
}
