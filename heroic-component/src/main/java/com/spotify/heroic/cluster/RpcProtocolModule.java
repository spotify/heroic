package com.spotify.heroic.cluster;

import com.google.inject.Key;
import com.google.inject.Module;

public interface RpcProtocolModule {
    public Module module(final Key<RpcProtocol> key);

    public String scheme();
}