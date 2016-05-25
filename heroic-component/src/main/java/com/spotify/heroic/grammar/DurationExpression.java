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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.common.Duration;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.TimeUnit;

@Data
@EqualsAndHashCode(exclude = {"ctx"})
@JsonTypeName("duration")
@RequiredArgsConstructor
public class DurationExpression implements Expression {
    private static final BinaryOperation ADD = (long a, long b) -> a + b;
    private static final BinaryOperation SUB = (long a, long b) -> a - b;

    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final TimeUnit unit;
    private final long value;

    @JsonCreator
    public DurationExpression(
        @JsonProperty("unit") final TimeUnit unit, @JsonProperty("value") final long value
    ) {
        this(Context.empty(), unit, value);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitDuration(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    @Override
    public DurationExpression sub(Expression other) {
        return operate(SUB, other);
    }

    @Override
    public DurationExpression add(Expression other) {
        return operate(ADD, other);
    }

    @Override
    public DurationExpression divide(final Expression other) {
        final long den = other.cast(IntegerExpression.class).getValue();

        long value = this.value;
        TimeUnit unit = this.unit;

        outer:
        while (value % den != 0) {
            if (unit == TimeUnit.MILLISECONDS) {
                break;
            }

            final TimeUnit next = nextSmallerUnit(unit);
            value = next.convert(value, unit);
            unit = next;
        }

        return new DurationExpression(ctx, unit, value / den);
    }

    private TimeUnit nextSmallerUnit(final TimeUnit unit) {
        switch (unit) {
            case DAYS:
                return TimeUnit.HOURS;
            case HOURS:
                return TimeUnit.MINUTES;
            case MINUTES:
                return TimeUnit.SECONDS;
            case SECONDS:
                return TimeUnit.MILLISECONDS;
            default:
                throw new IllegalArgumentException("No supported smaller unit: " + unit);
        }
    }

    @Override
    public DurationExpression negate() {
        return new DurationExpression(ctx, unit, -value);
    }

    private DurationExpression operate(BinaryOperation op, Expression other) {
        final DurationExpression o = other.cast(DurationExpression.class);

        final Context c = ctx.join(other.context());

        if (unit == o.unit) {
            return new DurationExpression(c, unit, op.calculate(value, o.value));
        }

        // decide which unit to convert to depending on which has the greatest magnitude in
        // milliseconds.
        if (unit.toMillis(1) < o.unit.toMillis(1)) {
            return new DurationExpression(c, unit,
                op.calculate(value, unit.convert(o.value, o.unit)));
        }

        return new DurationExpression(c, o.unit,
            op.calculate(o.unit.convert(value, unit), o.value));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(DurationExpression.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(IntegerExpression.class)) {
            return (T) new IntegerExpression(ctx, this.toMilliseconds());
        }

        throw ctx.castError(this, to);
    }

    public Duration toDuration() {
        return new Duration(value, unit);
    }

    public long toMilliseconds() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }

    @Override
    public String toString() {
        return String.format("<%d%s>", value, Duration.unitSuffix(unit));
    }

    private interface BinaryOperation {
        long calculate(long a, long b);
    }
}
