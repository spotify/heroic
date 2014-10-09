package com.spotify.heroic.metadata;

public class MetadataOperationException extends Exception {
    private static final long serialVersionUID = -3244781365564733792L;

    public MetadataOperationException(String string) {
        super(string);
    }

    public MetadataOperationException(String string, Throwable cause) {
        super(string, cause);
    }
}
