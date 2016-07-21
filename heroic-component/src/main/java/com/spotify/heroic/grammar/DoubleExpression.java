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

import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Data;

import java.util.concurrent.TimeUnit;

/**
 * int's are represented internally as longs.
 *
 * @author udoprog
 */
@Data
@JsonTypeName("double")
public final class DoubleExpression implements Expression {
    private final Context context;
    private final double value;

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitDouble(this);
    }

    @Override
    public DoubleExpression multiply(Expression other) {
        return new DoubleExpression(context.join(other.getContext()),
            value * other.cast(DoubleExpression.class).value);
    }

    @Override
    public DoubleExpression divide(Expression other) {
        return new DoubleExpression(context.join(other.getContext()),
            value / other.cast(DoubleExpression.class).value);
    }

    @Override
    public DoubleExpression sub(Expression other) {
        return new DoubleExpression(context.join(other.getContext()),
            value - other.cast(DoubleExpression.class).value);
    }

    @Override
    public DoubleExpression add(Expression other) {
        return new DoubleExpression(context.join(other.getContext()),
            value + other.cast(DoubleExpression.class).value);
    }

    @Override
    public DoubleExpression negate() {
        return new DoubleExpression(context, -value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends Expression> T cast(Class<T> to) {
        if (to.isAssignableFrom(DoubleExpression.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(IntegerExpression.class)) {
            return (T) new IntegerExpression(context, ((Double) value).longValue());
        }

        if (to.isAssignableFrom(DurationExpression.class)) {
            return (T) new DurationExpression(context, TimeUnit.MILLISECONDS, (long) value);
        }

        throw context.castError(this, to);
    }

    @Override
    public String toRepr() {
        return Double.toString(value);
    }

    @Override
    public String toString() {
        return "<" + toRepr() + " " + context + ">";
    }
}
