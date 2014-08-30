package com.spotify.heroic.yaml;

import lombok.Getter;

public class ValidationException extends Exception {
    private static final long serialVersionUID = -8053371486081707276L;

    @Getter
    private final ConfigContext context;

    public ValidationException(ConfigContext context, String message) {
        super(message);
        this.context = context;
    }

    public ValidationException(ConfigContext context, String message,
            Throwable cause) {
        super(message, cause);
        this.context = context;
    }
}
