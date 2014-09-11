package com.spotify.heroic.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "value" }, doNotUseGetters = true)
public class MatchKeyFilter implements OneTermFilter<String> {
    public static final String OPERATOR = "key";

    public static final OneTermFilterBuilder<MatchKeyFilter, String> BUILDER = new OneTermFilterBuilder<MatchKeyFilter, String>() {
        @Override
        public MatchKeyFilter build(String first) {
            return new MatchKeyFilter(first);
        }
    };

    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + value + "]";
    }

    @Override
    public MatchKeyFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public String first() {
        return value;
    }
}
