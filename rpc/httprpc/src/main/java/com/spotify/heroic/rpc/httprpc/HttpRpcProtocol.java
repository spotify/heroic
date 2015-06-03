package com.spotify.heroic.rpc.httprpc;

import java.net.URI;

import javax.inject.Inject;

import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.httpclient.HttpClientManager;
import com.spotify.heroic.httpclient.HttpClientSession;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

public class HttpRpcProtocol implements RpcProtocol {
    @Inject
    private AsyncFramework async;

    @Inject
    private HttpClientManager clients;

    @Override
    public AsyncFuture<ClusterNode> connect(final URI uri) {
        final HttpClientSession client = clients.newSession(uri, "rpc");

        final Transform<HttpRpcMetadata, ClusterNode> transform = new Transform<HttpRpcMetadata, ClusterNode>() {
            @Override
            public ClusterNode transform(final HttpRpcMetadata r) throws Exception {
                final NodeMetadata m = new NodeMetadata(r.getVersion(), r.getId(), r.getTags(), r.getCapabilities());
                return buildClusterNode(m);
            }

            /**
             * Pick the best cluster node implementation depending on the provided metadata.
             */
            private ClusterNode buildClusterNode(NodeMetadata m) throws Exception {
                /* Only create a client for the highest possibly supported version. */
                final HttpClientSession client = clients.newSession(uri, "rpc");
                return new HttpRpcResource.HttpRpcClusterNode(async, uri, client, m);
            }
        };

        return client.get(HttpRpcMetadata.class, "metadata").transform(transform);
    }
}