package com.spotify.heroic.backend;

public class BackendException extends Exception {
    private static final long serialVersionUID = -1520191315472136404L;

    public BackendException() {
    }

    public BackendException(String arg0) {
        super(arg0);
    }

    public BackendException(Throwable arg0) {
        super(arg0);
    }

    public BackendException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public BackendException(String arg0, Throwable arg1, boolean arg2,
            boolean arg3) {
        super(arg0, arg1, arg2, arg3);
    }
}
