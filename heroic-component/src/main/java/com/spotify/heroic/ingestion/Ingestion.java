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
import java.util.Iterator;
import java.util.List;
import lombok.Data;

@Data
public class Ingestion {
    private final List<RequestError> errors;

    /**
     * Build an ingestion instance from the given trace.
     *
     * @return an {@link com.spotify.heroic.ingestion.Ingestion}
     */
    public static Ingestion of() {
        return new Ingestion(ImmutableList.of());
    }

    /**
     * Build a collector that reduces multiple Ingestion instances into a single one.
     *
     * @return an {@link com.spotify.heroic.ingestion.Ingestion}
     */
    public static Collector<Ingestion, Ingestion> reduce() {
        return results -> {
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();

            for (final Ingestion r : results) {
                errors.addAll(r.errors);
            }

            return new Ingestion(errors.build());
        };
    }

    /**
     * Build a function that captured a shard error as an Ingestion.
     *
     * @param shard Shard where the error would originate from.
     * @return a {@link eu.toolchain.async.Transform}
     */
    public static Transform<Throwable, Ingestion> shardError(final ClusterShard shard) {
        return e -> new Ingestion(ImmutableList.of(ShardError.fromThrowable(shard, e)));
    }

    /**
     * Convert a {@link com.spotify.heroic.suggest.WriteSuggest} into an {@link
     * com.spotify.heroic.ingestion.Ingestion}.
     *
     * @param writeSuggest Write to convert.
     * @return an {@link com.spotify.heroic.ingestion.Ingestion}
     */
    public static Ingestion fromWriteSuggest(final WriteSuggest writeSuggest) {
        return new Ingestion(writeSuggest.getErrors());
    }

    /**
     * Convert a {@link com.spotify.heroic.metadata.WriteMetadata} into an {@link
     * com.spotify.heroic.ingestion.Ingestion}.
     *
     * @param writeMetadata Write to convert.
     * @return an {@link com.spotify.heroic.ingestion.Ingestion}
     */
    public static Ingestion fromWriteMetadata(final WriteMetadata writeMetadata) {
        return new Ingestion(writeMetadata.getErrors());
    }

    /**
     * Convert a {@link com.spotify.heroic.metric.WriteMetric} into an {@link
     * com.spotify.heroic.ingestion.Ingestion}.
     *
     * @param writeMetric Write to convert.
     * @return an {@link com.spotify.heroic.ingestion.Ingestion}
     */
    public static Ingestion fromWriteMetric(final WriteMetric writeMetric) {
        return new Ingestion(writeMetric.getErrors());
    }

    /**
     * The request to perform an ingestion.
     */
    @Data
    public static class Request {
        private final WriteOptions options;
        private final Series series;
        private final MetricCollection data;

        public Iterator<? extends Metric> all() {
            return data.getData().iterator();
        }
    }
}
