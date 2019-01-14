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

package com.spotify.heroic.filter;

import com.spotify.heroic.common.BiConsumerIO;
import com.spotify.heroic.common.FunctionIO;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

@RequiredArgsConstructor
public class OneArgumentFilterEncoding<T extends Filter, A> implements FilterEncoding<T> {
    private final Function<A, T> builder;
    private final Function<T, A> firstArgument;

    private final FunctionIO<Decoder, Optional<A>> decodeFirst;
    private final BiConsumerIO<Encoder, A> encodeFirst;

    public OneArgumentFilterEncoding(
        final Function<A, T> builder, final Function<T, A> firstArgument,
        final FilterEncodingComponent<A> first
    ) {
        this.firstArgument = firstArgument;
        this.builder = builder;

        this.decodeFirst = first.getDecoder();
        this.encodeFirst = first.getEncoder();
    }

    @Override
    public T deserialize(Decoder decoder) throws IOException {
        final A first = decodeFirst
            .apply(decoder)
            .orElseThrow(() -> new IllegalStateException("Expected one argument"));

        return builder.apply(first);
    }

    @Override
    public void serialize(Encoder encoder, T filter) throws IOException {
        encodeFirst.accept(encoder, firstArgument.apply(filter));
    }
}
