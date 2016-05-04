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

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.concurrent.TimeUnit;

/**
 * int's are represented internally as longs.
 *
 * @author udoprog
 */
@ValueName("int")
@Data
@EqualsAndHashCode(exclude = {"c"})
public final class IntValue implements Value {
    private final Long value;
    private final Context c;

    @Override
    public Context context() {
        return c;
    }

    @Override
    public Value sub(Value other) {
        return new IntValue(value - other.cast(this).value, c.join(other.context()));
    }

    @Override
    public Value add(Value other) {
        return new IntValue(value + other.cast(this).value, c.join(other.context()));
    }

    public String toString() {
        return String.format("<%d>", value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof IntValue) {
            return (T) this;
        }

        if (to instanceof DurationValue) {
            final DurationValue o = (DurationValue) to;
            return (T) new DurationValue(o.getUnit(),
                o.getUnit().convert(value, TimeUnit.MILLISECONDS), c);
        }

        throw c.castError(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(IntValue.class)) {
            return (T) this;
        }

        if (to.isAssignableFrom(Long.class)) {
            return (T) value;
        }

        if (to.isAssignableFrom(Double.class)) {
            return (T) (Double) value.doubleValue();
        }

        throw c.castError(this, to);
    }
}
