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

import java.io.IOException;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TwoArgumentFilterEncoding<T extends Filter, A, B> implements FilterEncoding<T> {
    private final Function<T, A> firstArgument;
    private final Function<T, B> secondArgument;

    private final BiFunction<A, B, T> builder;

    private final FunctionIO<Decoder, Optional<A>> decodeFirst;
    private final FunctionIO<Decoder, Optional<B>> decodeSecond;

    private final BiConsumerIO<Encoder, A> encodeFirst;
    private final BiConsumerIO<Encoder, B> encodeSecond;

    public TwoArgumentFilterEncoding(
        final BiFunction<A, B, T> builder, final Function<T, A> firstArgument,
        final Function<T, B> secondArgument, final FilterEncodingComponent<A> first,
        final FilterEncodingComponent<B> second
    ) {
        this.firstArgument = firstArgument;
        this.secondArgument = secondArgument;
        this.builder = builder;

        this.decodeFirst = first.getDecoder();
        this.decodeSecond = second.getDecoder();
        this.encodeFirst = first.getEncoder();
        this.encodeSecond = second.getEncoder();
    }

    @Override
    public T deserialize(Decoder decoder) throws IOException {
        final A first = decodeFirst
            .apply(decoder)
            .orElseThrow(() -> new IllegalStateException("Expected first argument"));

        final B second = decodeSecond
            .apply(decoder)
            .orElseThrow(() -> new IllegalStateException("Expected second argument"));

        return builder.apply(first, second);
    }

    @Override
    public void serialize(Encoder encoder, T filter) throws IOException {
        encodeFirst.accept(encoder, firstArgument.apply(filter));
        encodeSecond.accept(encoder, secondArgument.apply(filter));
    }
}
