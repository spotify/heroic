package com.spotify.heroic.rpc.httprpc;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.RpcProtocolModule;

@RequiredArgsConstructor
public class HttpRpcProtocolModule implements RpcProtocolModule {
    @JsonCreator
    public static HttpRpcProtocolModule create() {
        return new HttpRpcProtocolModule();
    }

    public static HttpRpcProtocolModule createDefault() {
        return create();
    }

    @Override
    public Module module(final Key<RpcProtocol> key) {
        return new PrivateModule() {
            @Override
            protected void configure() {
                bind(key).to(HttpRpcProtocol.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String scheme() {
        return "http";
    }
}