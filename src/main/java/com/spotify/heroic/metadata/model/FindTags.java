package com.spotify.heroic.metadata.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;

@Data
public class FindTags {
    public static final FindTags EMPTY = new FindTags(
            new HashMap<String, Set<String>>(), 0);

    private final Map<String, Set<String>> tags;
    private final int size;

    /**
     * Handle that tags is a deeply nested structure and copy it up until the
     * closest immutable type.
     */
    private static void updateTags(final Map<String, Set<String>> data,
            final Map<String, Set<String>> add) {
        for (final Map.Entry<String, Set<String>> entry : add.entrySet()) {
            Set<String> entries = data.get(entry.getKey());

            if (entries == null) {
                entries = new HashSet<String>();
                data.put(entry.getKey(), entries);
            }

            entries.addAll(entry.getValue());
        }
    }

    @Slf4j
    public static class Reducer implements Callback.Reducer<FindTags, FindTags> {
        private Reducer() {
        }

        @Override
        public FindTags resolved(Collection<FindTags> results,
                Collection<Exception> errors, Collection<CancelReason> cancelled)
                throws Exception {
            for (final Exception e : errors)
                log.error("Query failed", e);

            if (!errors.isEmpty() || !cancelled.isEmpty())
                throw new Exception("Query failed");

            final HashMap<String, Set<String>> tags = new HashMap<String, Set<String>>();
            int size = 0;

            for (final FindTags r : results) {
                updateTags(tags, r.getTags());
                size += r.getSize();
            }

            return new FindTags(tags, size);
        }
    }

    private static final Reducer reducer = new Reducer();

    public static Reducer reduce() {
        return reducer;
    }
}