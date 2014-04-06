package com.spotify.heroic.http.cache;

public interface CachedRequest<Q, R> {
    R run(Q query);
}