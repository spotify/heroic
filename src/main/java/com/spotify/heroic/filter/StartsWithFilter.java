package com.spotify.heroic.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = { "OPERATOR", "tag", "value" }, doNotUseGetters = true)
public class StartsWithFilter implements TwoTermsFilter {
    public static final String OPERATOR = "^";

    public static final TwoTermsFilterBuilder<StartsWithFilter> BUILDER = new TwoTermsFilterBuilder<StartsWithFilter>() {
        @Override
        public StartsWithFilter build(String first, String second) {
            return new StartsWithFilter(first, second);
        }
    };

    private final String tag;
    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + ", " + value + "]";
    }

    @Override
    public StartsWithFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public String first() {
        return tag;
    }

    @Override
    public String second() {
        return value;
    }
}
