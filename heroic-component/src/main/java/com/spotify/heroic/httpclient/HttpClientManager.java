package com.spotify.heroic.httpclient;

import java.net.URI;

public interface HttpClientManager {
    public HttpClientSession newSession(URI uri, String base);
}
