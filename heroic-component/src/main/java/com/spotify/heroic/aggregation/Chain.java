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

package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.ListIterator;

public class Chain implements Aggregation {
    public static final String NAME = "chain";

    private final List<Aggregation> chain;

    public Chain(@JsonProperty("chain") List<Aggregation> chain) {
        this.chain = chain;
    }

    public List<Aggregation> getChain() {
        return chain;
    }

    @Override
    public AggregationInstance apply(final AggregationContext context) {
        ListIterator<Aggregation> it = chain.listIterator(chain.size());

        AggregationContext current = context;
        final ImmutableSet.Builder<String> tags = ImmutableSet.builder();

        final ImmutableList.Builder<AggregationInstance> chain = ImmutableList.builder();

        while (it.hasPrevious()) {
            final AggregationInstance instance = it.previous().apply(current);
            tags.addAll(instance.requiredTags());
            current = current.withRequiredTags(tags.build());
            chain.add(instance);
        }

        return ChainInstance.fromList(Lists.reverse(chain.build()));
    }

    public static Aggregation fromList(final List<Aggregation> chain) {
        final List<Aggregation> c = flattenChain(chain);

        if (c.size() == 1) {
            return c.iterator().next();
        }

        return new Chain(c);
    }

    private static List<Aggregation> flattenChain(
        final List<Aggregation> chain
    ) {
        final ImmutableList.Builder<Aggregation> child = ImmutableList.builder();

        for (final Aggregation a : chain) {
            if (a instanceof Chain) {
                child.addAll(flattenChain(Chain.class.cast(a).getChain()));
            } else {
                child.add(a);
            }
        }

        return child.build();
    }
}
