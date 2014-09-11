package com.spotify.heroic.filter;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(of = { "OPERATOR" }, doNotUseGetters = true)
public class FalseFilter implements NoTermFilter {
    public static final String OPERATOR = "false";

    private FalseFilter() {
    }

    private static final FalseFilter instance = new FalseFilter();

    public static FalseFilter get() {
        return instance;
    }

    public static NoTermFilterBuilder<FalseFilter> BUILDER = new NoTermFilterBuilder<FalseFilter>() {
        @Override
        public FalseFilter build() {
            return instance;
        }
    };

    @Override
    public String toString() {
        return "[" + OPERATOR + "]";
    }

    @Override
    public FalseFilter optimize() {
        return this;
    }

    @Override
    public String operator() {
        return OPERATOR;
    }

    @Override
    public Filter invert() {
        return TrueFilter.get();
    }
}
