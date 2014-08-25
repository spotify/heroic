package com.spotify.heroic.model.filter;

import lombok.Data;

@Data
public class MatchKeyFilter implements Filter {
    public static final String OPERATOR = "key";

    private final String value;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + value + "]";
    }

    @Override
    public MatchKeyFilter optimize() {
        return this;
    }
}
