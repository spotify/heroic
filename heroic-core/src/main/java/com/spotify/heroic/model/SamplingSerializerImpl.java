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

package com.spotify.heroic.model;

import java.io.IOException;

import lombok.RequiredArgsConstructor;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public class SamplingSerializerImpl implements SamplingSerializer {
    private final Serializer<Long> longS;

    @Override
    public void serialize(SerialWriter buffer, Sampling value) throws IOException {
        longS.serialize(buffer, value.getSize());
        longS.serialize(buffer, value.getExtent());
    }

    @Override
    public Sampling deserialize(SerialReader buffer) throws IOException {
        final long size = longS.deserialize(buffer);
        final long extent = longS.deserialize(buffer);
        return new Sampling(size, extent);
    }
}