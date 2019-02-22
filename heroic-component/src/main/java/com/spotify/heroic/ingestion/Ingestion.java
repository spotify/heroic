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

package com.spotify.heroic.ingestion;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.suggest.WriteSuggest;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.Iterator;
import java.util.List;

@Data
public class Ingestion {
    public static final Ingestion EMPTY = new Ingestion(ImmutableList.of(), ImmutableList.of());

    private final List<RequestError> errors;
    private final List<Long> times;

    public static Ingestion of(final List<Long> times) {
        return new Ingestion(ImmutableList.of(), times);
    }

    public static Ingestion of(long duration) {
        return of(ImmutableList.of(duration));
    }

    public static Collector<Ingestion, Ingestion> reduce() {
        return results -> {
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final ImmutableList.Builder<Long> times = ImmutableList.builder();

            for (final Ingestion r : results) {
                errors.addAll(r.errors);
                times.addAll(r.times);
            }

            return new Ingestion(errors.build(), times.build());
        };
    }

    public static Transform<Throwable, Ingestion> shardError(final ClusterShard shard) {
        return e -> new Ingestion(ImmutableList.of(ShardError.fromThrowable(shard, e)),
            ImmutableList.of());
    }

    public static Ingestion fromWriteSuggest(final WriteSuggest writeSuggest) {
        return new Ingestion(writeSuggest.getErrors(), writeSuggest.getTimes());
    }

    public static Ingestion fromWriteMetadata(final WriteMetadata writeMetadata) {
        return new Ingestion(writeMetadata.getErrors(), writeMetadata.getTimes());
    }

    public static Ingestion fromWriteMetric(final WriteMetric writeMetric) {
        return new Ingestion(writeMetric.getErrors(), writeMetric.getTimes());
    }

    @Data
    public static class Request {
        private final Series series;
        private final MetricCollection data;

        public Iterator<? extends Metric> all() {
            return data.data().iterator();
        }
    }
}
