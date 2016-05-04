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

package com.spotify.heroic.grammar;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Iterator;
import java.util.Map;

@ValueName("aggregation")
@Data
@EqualsAndHashCode(exclude = {"c"})
public class AggregationValue implements Value {
    private final String name;
    private final ListValue arguments;
    private final Map<String, Value> keywordArguments;
    private final Context c;

    @Override
    public Context context() {
        return c;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof AggregationValue) {
            return (T) this;
        }

        throw c.castError(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(AggregationValue.class)) {
            return (T) this;
        }

        throw c.castError(this, to);
    }

    @Override
    public String toString() {
        final Joiner args = Joiner.on(", ");
        final Iterator<String> a =
            this.arguments.getList().stream().map(v -> v.toString()).iterator();
        final Iterator<String> k = keywordArguments
            .entrySet()
            .stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .iterator();
        return "" + name + "(" + args.join(Iterators.concat(a, k)) + ")";
    }

    public Aggregation build(final AggregationFactory aggregations) {
        return aggregations.build(name, arguments, keywordArguments);
    }
}
