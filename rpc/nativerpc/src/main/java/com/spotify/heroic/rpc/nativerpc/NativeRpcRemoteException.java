package com.spotify.heroic.rpc.nativerpc;

import java.net.InetSocketAddress;

import lombok.Getter;
import lombok.ToString;

@ToString(of = { "address", "message" })
public class NativeRpcRemoteException extends Exception {
    private static final long serialVersionUID = -664905544594225316L;

    @Getter
    public final InetSocketAddress address;

    public final String message;

    public NativeRpcRemoteException(InetSocketAddress address, String message) {
        super(address + ": " + message);
        this.address = address;
        this.message = message;
    }
}