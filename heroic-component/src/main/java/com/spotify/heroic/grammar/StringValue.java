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

@ValueName("string")
@Data
public final class StringValue implements Value {
    private final String string;

    @Override
    public Value sub(Value other) {
        throw new IllegalArgumentException(String.format("subtraction with string is not supported", this.getClass(),
                other.getClass()));
    }

    @Override
    public Value add(Value other) {
        return new StringValue(string + other.cast(this).string);
    }

    public String toString() {
        return String.format("<string:%s>", string);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(T to) {
        if (to instanceof StringValue)
            return (T) this;

        if (to instanceof Long) {
            try {
                return (T) Long.valueOf(string);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("not a valid int");
            }
        }

        throw new ValueCastException(this, to);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T cast(Class<T> to) {
        if (to.isAssignableFrom(StringValue.class))
            return (T) this;

        if (to == String.class)
            return (T) string;

        throw new ValueTypeCastException(this, to);
    }
}