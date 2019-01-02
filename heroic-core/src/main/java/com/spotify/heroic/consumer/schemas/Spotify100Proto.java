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

package com.spotify.heroic.consumer.schemas;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.ConsumerSchema;
import com.spotify.heroic.consumer.ConsumerSchemaException;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.consumer.SchemaScope;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.time.Clock;
import com.spotify.proto.Spotify100;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import javax.inject.Inject;
/**
 * Spotify100Proto is useful if you prefer not to batch and compress metrics with the
 * Spotify100ProtoBatch serializer.
 */
public class Spotify100Proto implements ConsumerSchema {

  @SchemaScope
  public static class Consumer implements ConsumerSchema.Consumer {

    private final Clock clock;
    private final IngestionGroup ingestion;
    private final ConsumerReporter reporter;

    @Inject
    public Consumer(Clock clock, IngestionGroup ingestion, ConsumerReporter reporter) {
      this.clock = clock;
      this.ingestion = ingestion;
      this.reporter = reporter;
    }

    @Override
    public AsyncFuture<Void> consume(final byte[] message) throws ConsumerSchemaException {
      // With protobuf, scalars such as strings and longs will never be null.
      final Spotify100.Metric metric;
      try {
         metric = Spotify100.Metric.parseFrom(message);
      } catch (InvalidProtocolBufferException e) {
        throw new ConsumerSchemaValidationException("Invalid metric", e);
      }

      if (metric.getTime() <= 0) {
        throw new ConsumerSchemaValidationException(
          "time: field must be a positive number: " + metric.toString());
      }

      if (metric.getKey().isEmpty()) {
        throw new ConsumerSchemaValidationException(
          "key: field must be defined: " + metric.toString());
      }

      final Series s = Series.of(metric.getKey(), metric.getTagsMap(), metric.getResourceMap());
      final Point p = new Point(metric.getTime(), metric.getValue());
      final List<Point> points = ImmutableList.of(p);

      reporter.reportMessageDrift(clock.currentTimeMillis() - p.getTimestamp());

      final AsyncFuture<Ingestion> ingestionFuture =
        ingestion.write(new Ingestion.Request(s, MetricCollection.points(points)));

      // Return Void future, to not leak unnecessary information from the backend but just
      // allow monitoring of when the consumption is done.
      return ingestionFuture.directTransform(future -> null);
    }
  }


  @Override
  public Exposed setup(final ConsumerSchema.Depends depends) {
    return DaggerSpotify100Proto_C.builder().depends(depends).build();
  }

  @SchemaScope
  @Component(dependencies = ConsumerSchema.Depends.class)
  interface C extends ConsumerSchema.Exposed {
    @Override
    Spotify100Proto.Consumer consumer();
  }
}
