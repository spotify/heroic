package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.apache.commons.lang.StringUtils;

@RequiredArgsConstructor
public final class ConfigContext {
    @Data
    public class Entry<T> {
        private final int index;
        private final T value;
        private final ConfigContext context;
    }

    private final List<String> path;

    public ConfigContext() {
        this.path = new ArrayList<String>();
    }

    public ConfigContext extend(String part) {
        final List<String> p = new ArrayList<>(this.path);
        p.add(part);
        return new ConfigContext(p);
    }

    public <T> Iterable<Entry<T>> iterate(final Collection<T> source,
            final String part) {
        if (source == null || source.isEmpty())
            return new ArrayList<Entry<T>>();

        return new Iterable<Entry<T>>() {
            @Override
            public Iterator<Entry<T>> iterator() {
                final Iterator<T> iterator = source.iterator();

                return new Iterator<Entry<T>>() {
                    int i = 0;

                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public Entry<T> next() {
                        final int index = i++;
                        return new Entry<T>(index, iterator.next(),
                                ConfigContext.this.extend(part + "[" + index
                                        + "]"));
                    }
                };
            }
        };
    }

    @Override
    public String toString() {
        return StringUtils.join(path, ".");
    }
}
