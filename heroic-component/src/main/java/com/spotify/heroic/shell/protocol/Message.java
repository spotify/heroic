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
    @SubType(Acknowledge.class)
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

        R visitCommandsRequest(CommandsRequest message) throws Exception;

        R visitRunTaskRequest(EvaluateRequest message) throws Exception;

        R visitFileOpened(FileOpened message) throws Exception;

        R visitFileReadResult(FileReadResult message) throws Exception;

        R visitOk(Acknowledge message) throws Exception;
    }
}
