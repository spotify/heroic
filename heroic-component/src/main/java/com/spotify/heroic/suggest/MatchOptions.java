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

package com.spotify.heroic.suggest;

import lombok.Data;

import java.beans.ConstructorProperties;
import java.util.Optional;

@Data
public class MatchOptions {
    public static final boolean DEFAULT_FUZZY = false;
    public static final int DEFAULT_FUZZY_PREFIX_LENGTH = 2;
    public static final int DEFAULT_FUZZY_MAX_EXPANSIONS = 20;
    public static final boolean DEFAULT_TOKENIZE = false;

    private final boolean fuzzy;
    private final int fuzzyPrefixLength;
    private final int fuzzyMaxExpansions;
    private final boolean tokenize;

    @ConstructorProperties({"fuzzy", "fuzzyPrefixLength", "fuzzyMaxExpansions", "tokenize"})
    public MatchOptions(
        final Optional<Boolean> fuzzy, final Optional<Integer> fuzzyPrefixLength,
        final Optional<Integer> fuzzyMaxExpansions, final Optional<Boolean> tokenize
    ) {
        this.fuzzy = fuzzy.orElse(DEFAULT_FUZZY);
        this.fuzzyPrefixLength = fuzzyPrefixLength.orElse(DEFAULT_FUZZY_PREFIX_LENGTH);
        this.fuzzyMaxExpansions = fuzzyMaxExpansions.orElse(DEFAULT_FUZZY_MAX_EXPANSIONS);
        this.tokenize = tokenize.orElse(DEFAULT_TOKENIZE);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Optional<Boolean> fuzzy = Optional.empty();
        private Optional<Integer> fuzzyPrefixLength = Optional.empty();
        private Optional<Integer> fuzzyMaxExpansions = Optional.empty();
        private Optional<Boolean> tokenize = Optional.empty();

        public Builder fuzzy(boolean fuzzy) {
            this.fuzzy = Optional.of(fuzzy);
            return this;
        }

        public Builder fuzzyPrefixLength(int fuzzyPrefixLength) {
            this.fuzzyPrefixLength = Optional.of(fuzzyPrefixLength);
            return this;
        }

        public Builder fuzzyMaxExpansions(int fuzzyMaxExpansions) {
            this.fuzzyMaxExpansions = Optional.of(fuzzyMaxExpansions);
            return this;
        }

        public Builder tokenize(boolean tokenize) {
            this.tokenize = Optional.of(tokenize);
            return this;
        }

        public MatchOptions build() {
            return new MatchOptions(fuzzy, fuzzyPrefixLength, fuzzyMaxExpansions, tokenize);
        }
    }
}
