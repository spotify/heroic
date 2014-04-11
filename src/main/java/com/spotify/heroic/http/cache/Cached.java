package com.spotify.heroic.http.cache;

import java.util.Date;

import lombok.Getter;
import lombok.ToString;

/**
 * A single cached item and its age.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type of the item to cache.
 */
@ToString(of = { "added" })
public class Cached<T> {
    @Getter
    private final T item;

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