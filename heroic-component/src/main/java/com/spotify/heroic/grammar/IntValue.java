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

/**
 * int's are represented internally as longs.
 *
 * @author udoprog
 */
@ValueName("int")
@Data
public final class IntValue implements Value {
    private final Long value;

    @Override
    public Value sub(Value other) {
        return new IntValue(value - other.cast(this).value);
    }

    @Override
    public Value add(Value other) {
        return new IntValue(value + other.cast(this).value);
    }

    public String toString() {
        return String.format("<int:%d>", value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof IntValue)
            return (T) this;

        if (to instanceof DiffValue) {
            final DiffValue o = (DiffValue) to;
            return (T) new DiffValue(o.getUnit(), o.getUnit().convert(value, TimeUnit.MILLISECONDS));
        }

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(IntValue.class))
            return (T) this;

        if (to.isAssignableFrom(Long.class))
            return (T) value;

        throw new ValueTypeCastException(this, to);
    }
}