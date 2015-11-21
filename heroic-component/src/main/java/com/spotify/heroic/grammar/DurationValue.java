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

package com.spotify.heroic.grammar;

import java.util.concurrent.TimeUnit;

import com.spotify.heroic.common.Duration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@ValueName("duration")
@Data
@EqualsAndHashCode(exclude = {"c"})
public class DurationValue implements Value {
    private static final BinaryOperation ADD = new BinaryOperation() {
        @Override
        public long calculate(long a, long b) {
            return a + b;
        }
    };

    private static final BinaryOperation SUB = new BinaryOperation() {
        @Override
        public long calculate(long a, long b) {
            return a - b;
        }
    };

    private final TimeUnit unit;
    private final long value;
    private final Context c;

    @Override
    public Context context() {
        return c;
    }

    @Override
    public Value sub(Value other) {
        return operate(SUB, other);
    }

    @Override
    public Value add(Value other) {
        return operate(ADD, other);
    }

    private Value operate(BinaryOperation op, Value other) {
        final DurationValue o = other.cast(this);

        final Context c = this.c.join(other.context());

        if (unit == o.unit) {
            return new DurationValue(unit, op.calculate(value, o.value), c);
        }

        // decide which unit to convert to depending on which has the greatest magnitude in
        // milliseconds.
        if (unit.toMillis(1) < o.unit.toMillis(1)) {
            return new DurationValue(unit, op.calculate(value, unit.convert(o.value, o.unit)), c);
        }

        return new DurationValue(o.unit, op.calculate(o.unit.convert(value, unit), o.value), c);
    }

    public String toString() {
        return String.format("<%d%s>", value, Duration.unitSuffix(unit));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof DurationValue) {
            return (T) this;
        }

        if (to instanceof IntValue) {
            return (T) new IntValue(TimeUnit.MILLISECONDS.convert(value, unit), c);
        }

        throw c.castError(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(DurationValue.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(Duration.class)) {
            return (T) this.toDuration();
        }

        throw c.castError(this, to);
    }

    public Duration toDuration() {
        return new Duration(value, unit);
    }

    public long toMilliseconds() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }

    private static interface BinaryOperation {
        long calculate(long a, long b);
    }
}
