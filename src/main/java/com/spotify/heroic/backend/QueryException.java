package com.spotify.heroic.backend;

public class QueryException extends BackendException {
    private static final long serialVersionUID = -4388927887766501517L;

    public QueryException() {
        super();
    }

    public QueryException(String arg0, Throwable arg1, boolean arg2,
            boolean arg3) {
        super(arg0, arg1, arg2, arg3);
    }

    public QueryException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public QueryException(String arg0) {
        super(arg0);
    }

    public QueryException(Throwable arg0) {
        super(arg0);
    }
}
