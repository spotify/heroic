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

import java.util.Optional;
import lombok.Data;

@Data
public class MatchOptions {
    private final boolean fuzzy;
    private final int fuzzyPrefixLength;
    private final int fuzzyMaxExpansions;
    private final boolean tokenize;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public static final boolean DEFAULT_FUZZY = false;
        public static final int DEFAULT_FUZZY_PREFIX_LENGTH = 2;
        public static final int DEFAULT_FUZZY_MAX_EXPANSIONS = 20;
        public static final boolean DEFAULT_TOKENIZE = false;

        private Optional<Boolean> fuzzy = Optional.empty();
        private Optional<Integer> fuzzyPrefixLength = Optional.empty();
        private Optional<Integer> fuzzyMaxExpansions = Optional.empty();
        private Optional<Boolean> tokenize = Optional.empty();

        @java.beans.ConstructorProperties({ "fuzzy", "fuzzyPrefixLength", "fuzzyMaxExpansions",
                                            "tokenize" })
        public Builder(final Optional<Boolean> fuzzy, final Optional<Integer> fuzzyPrefixLength,
                       final Optional<Integer> fuzzyMaxExpansions,
                       final Optional<Boolean> tokenize) {
            this.fuzzy = fuzzy;
            this.fuzzyPrefixLength = fuzzyPrefixLength;
            this.fuzzyMaxExpansions = fuzzyMaxExpansions;
            this.tokenize = tokenize;
        }

        public Builder() {
        }

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
            return new MatchOptions(fuzzy.orElse(DEFAULT_FUZZY),
                fuzzyPrefixLength.orElse(DEFAULT_FUZZY_PREFIX_LENGTH),
                fuzzyMaxExpansions.orElse(DEFAULT_FUZZY_MAX_EXPANSIONS),
                tokenize.orElse(DEFAULT_TOKENIZE));
        }
    }
}
