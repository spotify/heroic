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

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.RatioGauge;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SemanticRatioGauge extends RatioGauge {
    private final AtomicLong numerator = new AtomicLong();
    private final AtomicLong denominator = new AtomicLong();

    public SemanticRatioGauge(final long numerator, final long denominator) {
        this.numerator.set(numerator);
        this.denominator.set(denominator);
    }

    public void setNumerator(final long value) {
        numerator.set(value);
    }

    public void incNumerator() {
        numerator.incrementAndGet();
    }

    public void decNumerator() {
        numerator.decrementAndGet();
    }

    public void setDenominator(final long value) {
        denominator.set(value);
    }

    public void incDenominator() {
        denominator.incrementAndGet();
    }

    public void decDenominator() {
        denominator.decrementAndGet();
    }

    @Override
    protected Ratio getRatio() {
        return Ratio.of(numerator.get(), denominator.get());
    }
}
