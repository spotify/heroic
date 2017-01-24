/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.common;

import com.google.common.collect.TreeMultiset;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Utility class to build basic statistics about quantities.
 */
@Data
public class Histogram {
    private final Optional<Long> median;
    private final Optional<Long> p75;
    private final Optional<Long> p99;
    private final Optional<Long> min;
    private final Optional<Long> max;
    private final Optional<Double> mean;
    private final Optional<Long> sum;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final TreeMultiset<Long> samples = TreeMultiset.create(Long::compareTo);

        public void add(final long sample) {
            samples.add(sample);
        }

        public Histogram build() {
            final List<Long> entries = samples.stream().collect(Collectors.toList());

            final Optional<Long> median = nth(entries, 0.50);
            final Optional<Long> p75 = nth(entries, 0.75);
            final Optional<Long> p99 = nth(entries, 0.99);
            final Optional<Long> min = nth(entries, 0.00);
            final Optional<Long> max = nth(entries, 1.00);
            final Pair<Optional<Double>, Optional<Long>> mean = meanAndSum(entries);

            return new Histogram(median, p75, p99, min, max, mean.getLeft(), mean.getRight());
        }

        private Pair<Optional<Double>, Optional<Long>> meanAndSum(final List<Long> entries) {
            long sum = 0L;
            long count = 0L;

            for (final long value : entries) {
                sum += value;
                count += 1;
            }

            if (count <= 0L) {
                return Pair.of(Optional.empty(), Optional.empty());
            }

            final Optional<Double> mean = Optional.of((double) sum / (double) count);
            return Pair.of(mean, Optional.of(sum));
        }

        /**
         * Simplified accessor for n.
         */
        private Optional<Long> nth(final List<Long> samples, final double n) {
            if (samples.isEmpty()) {
                return Optional.empty();
            }

            final int index = Math.min(samples.size() - 1,
                Math.max(0, (int) Math.round(((double) samples.size()) * n) - 1));

            return Optional.of(samples.get(index));
        }
    }
}
