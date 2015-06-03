package com.spotify.heroic.grammar;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ValueCastException extends RuntimeException {
    private static final long serialVersionUID = -976720397372816419L;

    private final Object from;
    private final Object to;

    public ValueCastException(Object from, Object to) {
        super(String.format("%s cannot be cast to a compatible type of %s", from, to));

        this.from = from;
        this.to = to;
    }
}
