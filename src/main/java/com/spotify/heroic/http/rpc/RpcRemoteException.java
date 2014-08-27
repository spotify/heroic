package com.spotify.heroic.http.rpc;

public class RpcRemoteException extends Exception {
    private static final long serialVersionUID = 4669248130340709248L;

    public RpcRemoteException(String message) {
        super(message);
    }
}
