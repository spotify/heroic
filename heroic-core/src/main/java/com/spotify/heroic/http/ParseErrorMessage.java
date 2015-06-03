package com.spotify.heroic.http;

import lombok.Data;

@Data
public class ParseErrorMessage {
    private final String message;
    private final int line;
    private final int col;
    private final int lineEnd;
    private final int colEnd;
}