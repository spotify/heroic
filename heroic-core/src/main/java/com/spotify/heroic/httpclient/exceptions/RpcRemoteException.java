package com.spotify.heroic.httpclient.exceptions;

import java.net.URI;

import lombok.Getter;

public class RpcRemoteException extends RpcNodeException {
    private static final long serialVersionUID = 4669248130340709248L;

    @Getter
    private final String entity;

    public RpcRemoteException(URI uri, String message) {
        super(uri, message);
        this.entity = null;
    }

    public RpcRemoteException(URI uri, String message, String entity) {
        super(uri, formatMessage(message, entity));
        this.entity = entity;
    }

    private static String formatMessage(String message, Object entity) {
        final StringBuilder builder = new StringBuilder();
        builder.append(message).append(", entity: ").append(entity.toString());
        return builder.toString();
    }
}
