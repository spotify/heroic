package com.spotify.heroic.http.cache;

import java.util.Date;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.jaxrs.util.LRUMap;

/**
 * Setting up a handler for a single type of request.
 * 
 * Will use an LRU map to maintain caching for the most relevant responses.
 * 
 * @author udoprog
 * 
 * @param <Q>
 * @param <R>
 */
@Slf4j
public class CachedHandle<Q, R> {
    private final CachedRequest<Q, R> request;
    private final LRUMap<Q, Cached<R>> cache = new LRUMap<Q, Cached<R>>(10, 100);

    /* oldest allowed entry in a cache */
    private final long maxAge = 60000 * 5;

    /* time in ms a request should take to be cached. */
    private final long threshold = 100;

    public CachedHandle(CachedRequest<Q, R> request) {
        this.request = request;
    }

    public R run(Q query) {
        final Cached<R> entry = cache.get(query);

        if (entry != null && !entry.isOutdated(maxAge)) {
            log.debug("Cache hit (from {}): {}", entry, query);
            return entry.getItem();
        }

        log.debug("Cache miss: {}", query);

        final Date before = new Date();
        final R response = request.run(query);
        final Date after = new Date();

        if (after.getTime() - before.getTime() > threshold) {
            cache.put(query, new Cached<R>(response));
        }

        return response;
    }
}