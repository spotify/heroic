package com.spotify.heroic.http;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;

@Data
@ToString(exclude = { "uri", "base" })
public class HttpClientSession {
    private final ClientConfig config;
    private final ExecutorService executor;
    private final URI uri;
    private final String base;

    public <R, T> Callback<T> post(R request, Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return ConcurrentCallback.newResolve(executor,
                new HttpPostRequestResolver<R, T>(request, clazz, target));
    }

    public <T> Callback<T> get(Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return ConcurrentCallback.newResolve(executor,
                new HttpGetRequestResolver<T>(clazz, target));
    }
}
