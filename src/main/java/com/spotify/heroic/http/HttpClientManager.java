package com.spotify.heroic.http;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.Data;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

@Data
public class HttpClientManager {
    public static final int DEFAULT_THREAD_POOL_SIZE = 24;
    public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    public static final int DEFAULT_READ_TIMEOUT = 120000;

    private final ClientConfig config;
    private final ExecutorService executor;

    @JsonCreator
    public static HttpClientManager create(
            @JsonProperty("threadPoolSize") Integer threadPoolSize,
            @JsonProperty("connectTimeout") Integer connectTimeout,
            @JsonProperty("readTimeout") Integer readTimeout) {
        if (threadPoolSize == null)
            threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

        if (connectTimeout == null)
            connectTimeout = DEFAULT_CONNECT_TIMEOUT;

        if (readTimeout == null)
            readTimeout = DEFAULT_READ_TIMEOUT;

        final ClientConfig config = new ClientConfig();
        config.register(JacksonJsonProvider.class);

        config.property(ClientProperties.CONNECT_TIMEOUT, connectTimeout);
        config.property(ClientProperties.READ_TIMEOUT, readTimeout);

        final ExecutorService executor = Executors
                .newFixedThreadPool(threadPoolSize);

        return new HttpClientManager(config, executor);
    }

    /**
     * Create a default instance.
     */
    public static HttpClientManager create() {
        return create(null, null, null);
    }

    public HttpClientSession newSession(URI uri, String base) {
        return new HttpClientSession(config, executor, uri, base);
    }
}
