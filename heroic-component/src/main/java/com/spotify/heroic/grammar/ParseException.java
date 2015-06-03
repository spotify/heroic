package com.spotify.heroic.grammar;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ParseException extends RuntimeException {
    private static final long serialVersionUID = -7313640439644659488L;

    private final int line;
    private final int col;
    private final int lineEnd;
    private final int colEnd;

    public ParseException(String message, int line, int col) {
        this(message, null, line, col);
    }

    public ParseException(String message, Throwable cause, int line, int col) {
        this(message, cause, line, col, line, col);
    }

    public ParseException(String message, Throwable cause, int line, int col, int lineEnd, int colEnd) {
        super(String.format("%d:%d: %s", line, col, message), cause);
        this.line = line;
        this.col = col;
        this.lineEnd = lineEnd;
        this.colEnd = colEnd;
    }

    public String toString() {
        return "ParseException(message=" + getMessage() + ")";
    }
}
