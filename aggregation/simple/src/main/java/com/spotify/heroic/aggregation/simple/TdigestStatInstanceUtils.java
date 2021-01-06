/*
 * Copyright (c) 2020 Spotify AB.
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

package com.spotify.heroic.aggregation.simple;

import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * Utility class for Tdigest aggregation and computation.
 */
public class TdigestStatInstanceUtils {
    public static final double TDIGEST_COMPRESSION_LEVEL = 100.0;

    public static AtomicReference<TDigest> buildAtomicReference() {
        final TDigest tDigest = TDigest.createDigest(TDIGEST_COMPRESSION_LEVEL);
        return new AtomicReference<>(tDigest);
    }

    public static UnaryOperator<TDigest> getOp(final ByteBuffer serializedTDigest) {
        TDigest input = MergingDigest.fromBytes(serializedTDigest);
        return t -> {
            t.add(input);
            return t;
        };
    }

    public static UnaryOperator<TDigest> getOp(final TDigest input) {
        return t -> {
            t.add(input);
            return t;
        };
    }
}


