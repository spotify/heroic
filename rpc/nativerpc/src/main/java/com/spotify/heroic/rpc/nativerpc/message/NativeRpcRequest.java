package com.spotify.heroic.rpc.nativerpc.message;

import lombok.Data;

@Data
public class NativeRpcRequest {
    private final String endpoint;
    private final byte[] body;
}
