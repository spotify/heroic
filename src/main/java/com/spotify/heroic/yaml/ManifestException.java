package com.spotify.heroic.yaml;

public class ManifestException extends Exception {
    private static final long serialVersionUID = 5116935291993845390L;

    public ManifestException() {
        super();
    }

    public ManifestException(String message, Throwable cause) {
        super(message, cause);
    }

    public ManifestException(String message) {
        super(message);
    }

    public ManifestException(Throwable cause) {
        super(cause);
    }
}
