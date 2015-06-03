package com.spotify.heroic.suggest.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

@Data
public class MatchOptions {
    public static final boolean DEFAULT_FUZZY = true;
    public static final int DEFAULT_FUZZY_PREFIX_LENGTH = 2;
    public static final int DEFAULT_FUZZY_MAX_EXPANSIONS = 20;
    public static final boolean DEFAULT_TOKENIZE = false;

    private final boolean fuzzy;
    private final int fuzzyPrefixLength;
    private final int fuzzyMaxExpansions;
    private final boolean tokenize;

    @JsonCreator
    public MatchOptions(@JsonProperty("fuzzy") boolean fuzzy,
            @JsonProperty("fuzzyPrefixLength") Integer fuzzyPrefixLength,
            @JsonProperty("fuzzyMaxExpansions") Integer fuzzyMaxExpansions, @JsonProperty("tokenize") boolean tokenize) {
        this.fuzzy = Optional.fromNullable(fuzzy).or(DEFAULT_FUZZY);
        this.fuzzyPrefixLength = Optional.fromNullable(fuzzyPrefixLength).or(DEFAULT_FUZZY_PREFIX_LENGTH);
        this.fuzzyMaxExpansions = Optional.fromNullable(fuzzyMaxExpansions).or(DEFAULT_FUZZY_MAX_EXPANSIONS);
        this.tokenize = Optional.fromNullable(tokenize).or(DEFAULT_TOKENIZE);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean fuzzy = DEFAULT_FUZZY;
        private int fuzzyPrefixLength = DEFAULT_FUZZY_PREFIX_LENGTH;
        private int fuzzyMaxExpansions = DEFAULT_FUZZY_MAX_EXPANSIONS;
        private boolean tokenize = DEFAULT_TOKENIZE;

        public Builder fuzzy(boolean fuzzy) {
            this.fuzzy = fuzzy;
            return this;
        }

        public Builder fuzzyPrefixLength(int fuzzyPrefixLength) {
            this.fuzzyPrefixLength = fuzzyPrefixLength;
            return this;
        }

        public Builder fuzzyMaxExpansions(int fuzzyMaxExpansions) {
            this.fuzzyMaxExpansions = fuzzyMaxExpansions;
            return this;
        }

        public Builder tokenize(boolean tokenize) {
            this.tokenize = tokenize;
            return this;
        }

        public MatchOptions build() {
            return new MatchOptions(fuzzy, fuzzyPrefixLength, fuzzyMaxExpansions, tokenize);
        }
    }
}