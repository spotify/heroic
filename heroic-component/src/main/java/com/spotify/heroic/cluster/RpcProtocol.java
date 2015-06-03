package com.spotify.heroic.cluster;

import java.net.URI;

import eu.toolchain.async.AsyncFuture;

public interface RpcProtocol {
    public AsyncFuture<ClusterNode> connect(URI uri);
}
