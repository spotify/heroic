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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.consumer.ConsumerSchemaValidationException;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.time.Clock;
import com.spotify.proto.Spotify100.Batch;
import com.spotify.proto.Spotify100.Metric;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.xerial.snappy.Snappy;

@RunWith(MockitoJUnitRunner.class)
public class Spotify100ProtoTest {

  @Mock
  Clock clock;

  @Mock
  IngestionGroup ingestion;

  @Mock
  ConsumerReporter reporter;

  @Mock
  AsyncFramework async;

  @Mock
  private AsyncFuture<Ingestion> resolved;

  private Spotify100Proto.Consumer consumer;

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Before
  public void setup() {
    when(clock.currentTimeMillis()).thenReturn(1542830485000L);
    when(ingestion.write(any(Ingestion.Request.class))).thenReturn(resolved);
    consumer = new Spotify100Proto.Consumer(clock, ingestion, reporter, async);
  }

  @Test
  public void testConsumeBatchMetric() throws Exception {
    final Metric metric = Metric.newBuilder()
      .setKey("foo")
      .setValue(1.0)
      .setTime(1542830480000L)
      .putTags("tag1", "foo")
      .putResource("resource", "bar")
      .build();

    final Batch batch = Batch.newBuilder().addMetric(metric).build();

    consumer.consume(Snappy.compress(batch.toByteArray()));

    final Series s = Series.of(metric.getKey(), metric.getTagsMap(), metric.getResourceMap());
    final Point p = new Point(metric.getTime(), metric.getValue());
    final List<Point> points = ImmutableList.of(p);

    verify(reporter).reportMessageDrift(5000);
    verify(ingestion).write(new Ingestion.Request(s, MetricCollection.points(points)));
  }

  @Test
  public void testInvalidBatchMetrics() throws Exception {
    exceptionRule.expect(ConsumerSchemaValidationException.class);
    exceptionRule.expectMessage("Invalid batch of metrics");

    final Metric metric = Metric.newBuilder().setTime(-1542830480000L).build();

    consumer.consume(metric.toByteArray());
  }

  @Test
  public void testTimeValidationError() throws Exception {
    exceptionRule.expect(ConsumerSchemaValidationException.class);
    exceptionRule.expectMessage("time: field must be a positive number");

    final Metric metric = Metric.newBuilder().setTime(-1542830480000L).build();
    final Batch batch = Batch.newBuilder().addMetric(metric).build();

    consumer.consume(Snappy.compress(batch.toByteArray()));
  }


  @Test
  public void testKeyDefinedValidationError() throws Exception {
    exceptionRule.expect(ConsumerSchemaValidationException.class);
    exceptionRule.expectMessage("key: field must be defined");

    final Metric metric = Metric.newBuilder().setTime(1000L).build();
    final Batch batch = Batch.newBuilder().addMetric(metric).build();

    consumer.consume(Snappy.compress((batch.toByteArray())));
  }

}
