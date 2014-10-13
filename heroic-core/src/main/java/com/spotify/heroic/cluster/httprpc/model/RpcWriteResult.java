package com.spotify.heroic.cluster.httprpc.model;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class RpcWriteResult {
    private final boolean ok;

    @JsonCreator
    public static RpcWriteResult create(@JsonProperty(value = "ok", required = true) Boolean ok) {
        return new RpcWriteResult(ok);
    }
}
