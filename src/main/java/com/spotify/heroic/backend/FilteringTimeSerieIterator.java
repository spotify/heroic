package com.spotify.heroic.backend;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class FilteringTimeSerieIterator implements Iterator<TimeSerie> {
    private TimeSerie current;

    private final Iterator<TimeSerie> iterator;
    private final String key;
    private final Map<String, String> filter;
    private final Set<String> includes;

    public FilteringTimeSerieIterator(Iterator<TimeSerie> iterator, String key,
            Map<String, String> filter, Set<String> includes) {
        this.iterator = iterator;
        this.key = key;
        this.filter = filter;
        this.includes = includes;
    }

    private TimeSerie findNext() {
        while (iterator.hasNext()) {
            final TimeSerie next = iterator.next();

            if (key != null && !key.equals(next.getKey())) {
                continue;
            }

            if (filter != null
                    && !matchingTags(next.getTags(), filter, includes)) {
                continue;
            }

            return next;
        }

        return null;
    }

    @Override
    public boolean hasNext() {
        if (this.current == null) {
            this.current = findNext();
        }

        return current != null;
    }

    @Override
    public TimeSerie next() {
        if (this.current == null) {
            this.current = findNext();
        }

        final TimeSerie current = this.current;

        this.current = null;

        return current;
    }

    @Override
    public void remove() {
        throw new RuntimeException("remove not supported");
    }

    static boolean matchingTags(Map<String, String> tags,
            Map<String, String> filter, Set<String> includes) {
        // query not specified.
        if (filter != null) {
            // match the row tags with the query tags.
            for (final Map.Entry<String, String> entry : filter.entrySet()) {
                // check tags for the actual row.
                final String tagValue = tags.get(entry.getKey());

                if (tagValue == null || !tagValue.equals(entry.getValue())) {
                    return false;
                }
            }
        }

        if (includes != null) {
            for (final String tag : includes) {
                if (!tags.containsKey(tag)) {
                    return false;
                }
            }
        }

        return true;
    }
}