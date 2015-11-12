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

package com.spotify.heroic.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.metric.NodeError;
import com.spotify.heroic.metric.RequestError;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Data
public class DeleteSeries {
    public static final List<RequestError> EMPTY_ERRORS = ImmutableList.of();
    public static final DeleteSeries EMPTY = new DeleteSeries(EMPTY_ERRORS, 0, 0);

    private final List<RequestError> errors;
    private final int deleted;
    private final int failed;

    public DeleteSeries(int deleted, int failed) {
        this.errors = EMPTY_ERRORS;
        this.deleted = deleted;
        this.failed = failed;
    }

    @JsonCreator
    public DeleteSeries(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("deleted") int deleted, @JsonProperty("failed") int failed) {
        this.errors = errors;
        this.deleted = deleted;
        this.failed = failed;
    }

    private static final Collector<DeleteSeries, DeleteSeries> reducer =
            new Collector<DeleteSeries, DeleteSeries>() {
                @Override
                public DeleteSeries collect(Collection<DeleteSeries> results) throws Exception {
                    final List<RequestError> errors = new ArrayList<>();
                    int deleted = 0;
                    int failed = 0;

                    for (final DeleteSeries result : results) {
                        errors.addAll(result.errors);
                        deleted += result.getDeleted();
                        failed += result.getFailed();
                    }

                    return new DeleteSeries(errors, deleted, failed);
                }
            };

    public static Collector<DeleteSeries, DeleteSeries> reduce() {
        return reducer;
    }

    public static Transform<Throwable, DeleteSeries> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, DeleteSeries>() {
            @Override
            public DeleteSeries transform(Throwable e) throws Exception {
                final List<RequestError> errors =
                        ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(), e));
                return new DeleteSeries(errors, 0, 0);
            }
        };
    }
}
