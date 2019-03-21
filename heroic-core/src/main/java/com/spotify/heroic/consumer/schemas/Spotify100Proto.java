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
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import org.xerial.snappy.Snappy;

/**
 * Spotify100Proto is intended to reduce the amount of data transferred between the publisher
 * and the consumer. It useful when being used with Google Pubsub, because the client does not have
 * native compression like Kafka.
 *
 * Compression is done with the snappy library via JNI.
 */
public class Spotify100Proto implements ConsumerSchema {

  @SchemaScope
  public static class Consumer implements ConsumerSchema.Consumer {

    private final Clock clock;
    private final IngestionGroup ingestion;
    private final ConsumerReporter reporter;
    private final AsyncFramework async;

    @Inject
    public Consumer(
      Clock clock,
      IngestionGroup ingestion,
      ConsumerReporter reporter,
      AsyncFramework async
    ) {
      this.clock = clock;
      this.ingestion = ingestion;
      this.reporter = reporter;
      this.async = async;
    }

    @Override
    public AsyncFuture<Void> consume(final byte[] message) throws ConsumerSchemaException {
      final List<Spotify100.Metric> metrics;
      try {
        metrics = Spotify100.Batch.parseFrom(Snappy.uncompress(message)).getMetricList();
      } catch (IOException e) {
        throw new ConsumerSchemaValidationException("Invalid batch of metrics", e);
      }

      final List<AsyncFuture<Ingestion>> ingestions = new ArrayList<>();
      for (Spotify100.Metric metric : metrics) {

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

        ingestions.add(ingestion.write(new Ingestion.Request(s, MetricCollection.points(points))));
      }

      // Return Void future, to not leak unnecessary information from the backend but just
      // allow monitoring of when the consumption is done.
      return async.collectAndDiscard(ingestions);
    }
  }

  @Override
  public Exposed setup(final Depends depends) {
    return DaggerSpotify100Proto_C.builder().depends(depends).build();
  }

  @SchemaScope
  @Component(dependencies = Depends.class)
  interface C extends Exposed {
    @Override
    Spotify100Proto.Consumer consumer();
  }
}
