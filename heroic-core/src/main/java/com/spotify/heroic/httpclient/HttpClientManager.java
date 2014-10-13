package com.spotify.heroic.httpclient;

import java.net.URI;

import javax.inject.Inject;

import lombok.Data;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.concurrrency.ThreadPool;

@Data
public class HttpClientManager {
    public static final int DEFAULT_THREAD_POOL_SIZE = 100;
    public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    public static final int DEFAULT_READ_TIMEOUT = 120000;

    @Inject
    private ClientConfig config;

    @Inject
    private ThreadPool request;

    public HttpClientSession newSession(URI uri, String base) {
        return new HttpClientSession(config, request.get(), uri, base);
    }
}
