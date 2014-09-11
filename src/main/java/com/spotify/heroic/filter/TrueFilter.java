package com.spotify.heroic.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = { "OPERATOR" }, doNotUseGetters = true)
public class TrueFilter implements NoTermFilter {
    public static final String OPERATOR = "true";

    private TrueFilter() {
    }

    private static final TrueFilter instance = new TrueFilter();

    public static TrueFilter get() {
        return instance;
    }

    public static NoTermFilterBuilder<TrueFilter> BUILDER = new NoTermFilterBuilder<TrueFilter>() {
        @Override
        public TrueFilter build() {
            return instance;
        }
    };

    @Override
    public String toString() {
        return "[" + OPERATOR + "]";
    }

    @Override
    public TrueFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter invert() {
        return FalseFilter.get();
    }
}
