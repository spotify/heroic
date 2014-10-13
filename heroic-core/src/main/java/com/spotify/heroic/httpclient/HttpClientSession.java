package com.spotify.heroic.httpclient;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;

@Data
@ToString(exclude = { "uri", "base" })
public class HttpClientSession {
    private final ClientConfig config;
    private final ExecutorService executor;
    private final URI uri;
    private final String base;

    public <R, T> Future<T> post(R request, Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return Futures.resolve(executor, new HttpPostRequestResolver<R, T>(request, clazz, target));
    }

    public <T> Future<T> get(Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return Futures.resolve(executor, new HttpGetRequestResolver<T>(clazz, target));
    }
}
