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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {"base"})
@RequiredArgsConstructor
public class SemanticConsumerReporter implements ConsumerReporter {
    private static final String COMPONENT = "consumer";

    private final SemanticMetricRegistry registry;
    private final MetricId base;

    private final Meter messageIn;
    private final Meter messageError;
    private final Meter consumerSchemaError;
    private final SemanticRatioGauge consumerThreadsLiveRatio;
    private final Histogram messageSize;
    private final Histogram messageDrift;
    private final SemanticFutureReporter consumer;

    private final SemanticHeroicTimerGauge consumerCommitWholeOperationTimer;
    private final SemanticHeroicTimerGauge consumerCommitPhase1Timer;
    private final SemanticHeroicTimerGauge consumerCommitPhase2Timer;

    public SemanticConsumerReporter(SemanticMetricRegistry registry, String id) {
        this.registry = registry;

        this.base = MetricId.build().tagged("component", COMPONENT, "id", id);

        messageIn = registry.meter(base.tagged("what", "message-in", "unit", Units.MESSAGE));
        messageError = registry.meter(base.tagged("what", "message-error", "unit", Units.FAILURE));
        consumerSchemaError =
            registry.meter(base.tagged("what", "consumer-schema-error", "unit", Units.FAILURE));
        consumerThreadsLiveRatio = new SemanticRatioGauge();
        registry.register(base.tagged("what", "consumer-threads-live-ratio", "unit", Units.RATIO),
            consumerThreadsLiveRatio);
        messageSize = registry.histogram(base.tagged("what", "message-size", "unit", Units.BYTE));
        messageDrift =
            registry.histogram(base.tagged("what", "message-drift", "unit", Units.MILLISECOND));

        consumer = new SemanticFutureReporter(registry,
            base.tagged("what", "consumer", "unit", Units.WRITE));

        consumerCommitWholeOperationTimer =
            registry.register(base.tagged("what", "consumer-commit-latency"),
                new SemanticHeroicTimerGauge());
        consumerCommitPhase1Timer =
            registry.register(base.tagged("what", "consumer-commit-phase1-latency"),
                new SemanticHeroicTimerGauge());
        consumerCommitPhase2Timer =
            registry.register(base.tagged("what", "consumer-commit-phase2-latency"),
                new SemanticHeroicTimerGauge());
    }

    @Override
    public void reportMessageSize(int size) {
        messageIn.mark();
        messageSize.update(size);
    }

    @Override
    public void reportMessageError() {
        messageError.mark();
    }

    @Override
    public void reportConsumerSchemaError() {
        consumerSchemaError.mark();
    }

    @Override
    public void reportConsumerThreadsWanted(final long count) {
        consumerThreadsLiveRatio.setDenominator(count);
    }

    @Override
    public void reportConsumerThreadsIncrement() {
        consumerThreadsLiveRatio.incNumerator();
    }

    @Override
    public void reportConsumerThreadsDecrement() {
        consumerThreadsLiveRatio.decNumerator();
    }

    @Override
    public void reportMessageDrift(final long ms) {
        messageDrift.update(ms);
    }

    @Override
    public FutureReporter.Context reportConsumption() {
        return consumer.setup();
    }

    @Override
    public HeroicTimer.Context reportConsumerCommitOperation() {
        return consumerCommitWholeOperationTimer.time();
    }

    @Override
    public HeroicTimer.Context reportConsumerCommitPhase1() {
        return consumerCommitPhase1Timer.time();
    }

    @Override
    public HeroicTimer.Context reportConsumerCommitPhase2() {
        return consumerCommitPhase2Timer.time();
    }
}
