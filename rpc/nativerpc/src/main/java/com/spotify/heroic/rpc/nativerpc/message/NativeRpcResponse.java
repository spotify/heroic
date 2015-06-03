package com.spotify.heroic.rpc.nativerpc.message;

import lombok.Data;

@Data
public class NativeRpcResponse {
    private final byte[] body;
}
