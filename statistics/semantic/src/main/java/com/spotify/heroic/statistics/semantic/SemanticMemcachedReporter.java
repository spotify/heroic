/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Counter;
import com.spotify.heroic.statistics.MemcachedReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;

@ToString(of = {})
public class SemanticMemcachedReporter implements MemcachedReporter {

  private static final String COMPONENT = "memcached";

  private final Counter memcachedHit;
  private final Counter memcachedMiss;
  private final Counter memcachedTimeout;
  private final Counter memcachedError;

  public SemanticMemcachedReporter(SemanticMetricRegistry registry, final String consumerType) {
    final MetricId id = MetricId.build().tagged("component", COMPONENT, "consumer", consumerType);

    memcachedHit = registry.counter(id.tagged("what", "memcached-performance", "result",
        "hit"));
    memcachedMiss = registry.counter(id.tagged("what", "memcached-performance", "result",
        "miss"));
    memcachedTimeout = registry.counter(id.tagged("what", "memcached-performance", "result",
        "timeout"));

    memcachedError = registry.counter(id.tagged("what", "memcached-performance", "result",
        "error"));


  }


  @Override
  public void reportMemcachedHit() {
    memcachedHit.inc();
  }

  @Override
  public void reportMemcachedMiss() {
    memcachedMiss.inc();
  }

  @Override
  public void reportMemcachedTimeout() {
    memcachedTimeout.inc();
  }

  @Override
  public void reportMemcachedError() {
    memcachedError.inc();
  }
}

