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

import lombok.Data;
import lombok.EqualsAndHashCode;

@ValueName("diff")
@Data
@EqualsAndHashCode(of = { "unit", "value" })
public class DiffValue implements Value {
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

    @Override
    public Value sub(Value other) {
        return operate(SUB, other);
    }

    @Override
    public Value add(Value other) {
        return operate(ADD, other);
    }

    private Value operate(BinaryOperation op, Value other) {
        final DiffValue o = other.cast(this);

        if (unit == o.unit)
            return new DiffValue(unit, op.calculate(value, o.value));

        // decide which unit to convert to depending on which has the greatest magnitude in milliseconds.
        if (unit.toMillis(1) < o.unit.toMillis(1))
            return new DiffValue(unit, op.calculate(value, unit.convert(o.value, o.unit)));

        return new DiffValue(o.unit, op.calculate(o.unit.convert(value, unit), o.value));
    }

    public String toString() {
        return String.format("<diff:%s:%d>", unit, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof DiffValue)
            return (T) this;

        if (to instanceof IntValue)
            return (T) new IntValue(TimeUnit.MILLISECONDS.convert(value, unit));

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(DiffValue.class))
            return (T) this;

        throw new ValueTypeCastException(this, to);
    }

    public long toMilliseconds() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }

    private static interface BinaryOperation {
        long calculate(long a, long b);
    }
}