package com.spotify.heroic.http.cache;

import java.util.Date;

import lombok.Getter;

/**
 * A single cached item and its age.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type of the item to cache.
 */
public class Cached<T> {
    @Getter
    private final T item;
    @Getter
    private final Date added = new Date();

    public Cached(T item) {
        this.item = item;
    }

    public boolean isOutdated(long age) {
        final Date now = new Date();

        if (now.getTime() - added.getTime() > age) {
            return true;
        }

        return false;
    }
}