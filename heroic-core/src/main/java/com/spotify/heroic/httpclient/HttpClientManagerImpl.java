package com.spotify.heroic.httpclient;

import java.net.URI;

import javax.inject.Inject;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.concurrrency.ThreadPool;

import eu.toolchain.async.AsyncFramework;

public class HttpClientManagerImpl implements HttpClientManager {
    @Inject
    private AsyncFramework async;

    @Inject
    private ClientConfig config;

    @Inject
    private ThreadPool request;

    @Override
    public HttpClientSession newSession(URI uri, String base) {
        return new HttpClientSessionImpl(async, config, request.get(), uri, base);
    }
}
