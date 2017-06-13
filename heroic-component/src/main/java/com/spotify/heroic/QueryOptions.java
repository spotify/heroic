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

package com.spotify.heroic;

import com.spotify.heroic.aggregation.BucketStrategy;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.Tracing;
import java.util.Optional;
import lombok.Data;

@Data
public class QueryOptions {
    /**
     * Strategy for how to create buckets when performing a sampling aggregation.
     */
    private final Optional<BucketStrategy> bucketStrategy;

    /**
     * Indicates if tracing is enabled.
     * <p>
     * Traces queries will include a {@link QueryTrace} object that indicates detailed timings of
     * the query.
     *
     * @return {@code true} if tracing is enabled.
     */
    private final Optional<Tracing> tracing;

    /**
     * The number of entries to fetch for every batch.
     */
    private final Optional<Integer> fetchSize;

    /**
     * Limit the number of returned groups.
     */
    private final OptionalLimit dataLimit;

    /**
     * Limit the number of points retained by the aggregation.
     */
    private final OptionalLimit aggregationLimit;

    /**
     * Limit the number of returned groups.
     */
    private final OptionalLimit groupLimit;

    /**
     * Limit the number of series used.
     */
    private final OptionalLimit seriesLimit;

    /**
     * Report limiting as a failure.
     */
    private final Optional<Boolean> failOnLimits;

    public Tracing tracing() {
        return tracing.orElse(Tracing.DEFAULT);
    }

    public static QueryOptions defaults() {
        return new QueryOptions(Optional.empty(), Optional.empty(), Optional.empty(),
            OptionalLimit.empty(), OptionalLimit.empty(), OptionalLimit.empty(),
            OptionalLimit.empty(), Optional.empty());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<BucketStrategy> bucketStrategy = Optional.empty();
        private Optional<Tracing> tracing = Optional.empty();
        private Optional<Integer> fetchSize = Optional.empty();
        private OptionalLimit dataLimit = OptionalLimit.empty();
        private OptionalLimit aggregationLimit = OptionalLimit.empty();
        private OptionalLimit groupLimit = OptionalLimit.empty();
        private OptionalLimit seriesLimit = OptionalLimit.empty();
        private Optional<Boolean> failOnLimits = Optional.empty();

        public Builder bucketStrategy(BucketStrategy bucketStrategy) {
            this.bucketStrategy = Optional.of(bucketStrategy);
            return this;
        }

        public Builder tracing(Tracing tracing) {
            this.tracing = Optional.of(tracing);
            return this;
        }

        public Builder fetchSize(int fetchSize) {
            this.fetchSize = Optional.of(fetchSize);
            return this;
        }

        public Builder dataLimit(long dataLimit) {
            this.dataLimit = OptionalLimit.of(dataLimit);
            return this;
        }

        public Builder aggregationLimit(long aggregationLimit) {
            this.aggregationLimit = OptionalLimit.of(aggregationLimit);
            return this;
        }

        public Builder groupLimit(long groupLimit) {
            this.groupLimit = OptionalLimit.of(groupLimit);
            return this;
        }

        public Builder seriesLimit(long seriesLimit) {
            this.seriesLimit = OptionalLimit.of(seriesLimit);
            return this;
        }

        public Builder failOnLimits(boolean failOnLimits) {
            this.failOnLimits = Optional.of(failOnLimits);
            return this;
        }

        public QueryOptions build() {
            return new QueryOptions(bucketStrategy, tracing, fetchSize, dataLimit, aggregationLimit,
                groupLimit, seriesLimit, failOnLimits);
        }
    }
}
