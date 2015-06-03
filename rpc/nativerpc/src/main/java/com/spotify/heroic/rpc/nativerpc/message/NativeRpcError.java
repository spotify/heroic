package com.spotify.heroic.rpc.nativerpc.message;

import lombok.Data;

@Data
public class NativeRpcError {
    private final String message;
}