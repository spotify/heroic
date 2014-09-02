package com.spotify.heroic.model.filter;

import lombok.Data;

@Data
public class StartsWithFilter implements Filter {
    public static final String OPERATOR = "$";

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
}
