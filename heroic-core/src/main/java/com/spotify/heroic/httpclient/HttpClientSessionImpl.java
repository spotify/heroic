package com.spotify.heroic.httpclient;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Data
@ToString(exclude = { "uri", "base" })
public class HttpClientSessionImpl implements HttpClientSession {
    private final AsyncFramework async;
    private final ClientConfig config;
    private final ExecutorService executor;
    private final URI uri;
    private final String base;

    public <R, T> AsyncFuture<T> post(R request, Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return async.call(new HttpPostRequestResolver<R, T>(request, clazz, target), executor);
    }

    public <T> AsyncFuture<T> get(Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return async.call(new HttpGetRequestResolver<T>(clazz, target), executor);
    }
}
