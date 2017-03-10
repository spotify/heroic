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

package com.spotify.heroic.instrumentation;

import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Data;

@Data
public class OperationsLogImpl implements OperationsLog {
    public enum OpType {
        WRITE_REQUEST, WRITE_COMPLETE, OFFSETS_COMMIT
    }

    private final ConcurrentLinkedQueue<OpType> log = new ConcurrentLinkedQueue<>();

    @Override
    public void registerWriteRequest() {
        log.add(OpType.WRITE_REQUEST);
    }

    public void registerWriteComplete() {
        log.add(OpType.WRITE_COMPLETE);
    }

    public void registerConsumerOffsetsCommit() {
        log.add(OpType.OFFSETS_COMMIT);
    }
}

