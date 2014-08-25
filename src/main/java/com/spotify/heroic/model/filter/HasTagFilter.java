package com.spotify.heroic.model.filter;

import lombok.Data;

@Data
public class HasTagFilter implements Filter {
    public static final String OPERATOR = "+";
    private final String tag;

    @Override
    public String toString() {
        return "[" + OPERATOR + ", " + tag + "]";
    }

    @Override
    public HasTagFilter optimize() {
        return this;
    }
}
