package com.spotify.heroic.httpclient.exceptions;

import java.net.URI;

import lombok.Getter;

public class RpcNodeException extends Exception {
    private static final long serialVersionUID = 7605363267979193022L;

    @Getter
    private final URI uri;

    public RpcNodeException(URI uri, String message) {
        super(message);
        this.uri = uri;
    }

    public RpcNodeException(URI uri, String message, Throwable e) {
        super(message, e);
        this.uri = uri;
    }
}
