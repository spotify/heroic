package com.spotify.heroic.statistics.noop;

import com.spotify.heroic.statistics.HttpClientManagerReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;

public class NoopHttpClientManagerReporter implements HttpClientManagerReporter {
    private NoopHttpClientManagerReporter() {
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return NoopThreadPoolReporter.get();
    }

    private static final NoopHttpClientManagerReporter instance = new NoopHttpClientManagerReporter();

    public static NoopHttpClientManagerReporter get() {
        return instance;
    }
}
