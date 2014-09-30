package com.spotify.heroic.http;

import javax.inject.Inject;
import javax.inject.Singleton;

import lombok.Data;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.concurrrency.ThreadPool;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.HttpClientManagerReporter;

@Data
public class HttpClientManagerConfig {
    // 100 requests allowed to be pending.
    // these will be mostly blocking for a response.
    public static final int DEFAULT_THREADS = 100;
    // + 100 requests are allowed to be pending in queue.
    public static final int DEFAULT_QUEUE_SIZE = 100;
    // allow 2 seconds to connect
    public static final int DEFAULT_CONNECT_TIMEOUT = 2000;
    // allow each request to take at most 2 minutes.
    public static final int DEFAULT_READ_TIMEOUT = 120000;

    private final int threads;
    private final int queueSize;
    private final int connectTimeout;
    private final int readTimeout;

    @JsonCreator
    public static HttpClientManagerConfig create(
            @JsonProperty("threads") Integer threads,
            @JsonProperty("queueSize") Integer queueSize,
            @JsonProperty("connectTimeout") Integer connectTimeout,
            @JsonProperty("readTimeout") Integer readTimeout) {
        if (threads == null)
            threads = DEFAULT_THREADS;

        if (queueSize == null)
            queueSize = DEFAULT_QUEUE_SIZE;

        if (connectTimeout == null)
            connectTimeout = DEFAULT_CONNECT_TIMEOUT;

        if (readTimeout == null)
            readTimeout = DEFAULT_READ_TIMEOUT;

        return new HttpClientManagerConfig(threads, queueSize, connectTimeout,
                readTimeout);
    }

    /**
     * Create a default instance.
     */
    public static HttpClientManagerConfig create() {
        return create(null, null, null, null);
    }

    public Module module() {
        return new PrivateModule() {
            @Provides
            @Singleton
            public ClientConfig config() {
                final ClientConfig config = new ClientConfig();
                config.register(JacksonJsonProvider.class);

                config.property(ClientProperties.CONNECT_TIMEOUT,
                        connectTimeout);
                config.property(ClientProperties.READ_TIMEOUT, readTimeout);

                return config;
            }

            @Inject
            @Provides
            @Singleton
            public HttpClientManagerReporter reporter(HeroicReporter reporter) {
                return reporter.newHttpClientManager();
            }

            @Provides
            @Singleton
            public ThreadPool executor(HttpClientManagerReporter reporter) {
                return ThreadPool.create("request", reporter.newThreadPool(),
                        threads, queueSize);
            }

            @Override
            protected void configure() {
                bind(HttpClientManager.class).in(Scopes.SINGLETON);
                expose(HttpClientManager.class);
            }
        };
    }
}
