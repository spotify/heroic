package com.spotify.heroic.grammar;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ValueTypeCastException extends RuntimeException {
    private static final long serialVersionUID = 3125985026131597565L;

    private final Object from;
    private final Class<?> to;

    public ValueTypeCastException(Object from, Class<?> to) {
        super(String.format("%s cannot be cast to %s", from, to));

        this.from = from;
        this.to = to;
    }
}