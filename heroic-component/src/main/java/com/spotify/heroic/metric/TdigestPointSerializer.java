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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tdunning.math.stats.TDigest;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TdigestPointSerializer extends StdSerializer<TdigestPoint> {

    protected TdigestPointSerializer(Class<TdigestPoint> t) {
        super(t);
    }

    public TdigestPointSerializer() {
        this(TdigestPoint.class);
    }

    @Override
    public void serialize(TdigestPoint point,
                          JsonGenerator gen,
                          SerializerProvider provider) throws IOException {

        gen.writeStartObject();
        gen.writeObjectField("value",
            serialize(point.value()));
        gen.writeNumberField("timestamp", point.getTimestamp());
        gen.writeEndObject();

    }

    private byte[] serialize(TDigest tDigest) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(tDigest.smallByteSize());
        tDigest.asSmallBytes(byteBuffer);
        return byteBuffer.array();
    }
}
