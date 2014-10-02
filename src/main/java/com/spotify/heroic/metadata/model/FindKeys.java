package com.spotify.heroic.metadata.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;

@Data
public class FindKeys {
    public static final FindKeys EMPTY = new FindKeys(new HashSet<String>(), 0);

    private final Set<String> keys;
    private final int size;

    @Slf4j
    public static class Reducer implements Callback.Reducer<FindKeys, FindKeys> {
        @Override
        public FindKeys resolved(Collection<FindKeys> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception {
            for (final Exception e : errors)
                log.error("Query failed", e);

            if (!errors.isEmpty() || !cancelled.isEmpty())
                throw new Exception("Query failed");

            final Set<String> keys = new HashSet<>();
            int size = 0;

            for (final FindKeys r : results) {
                keys.addAll(r.getKeys());
                size += r.getSize();
            }

            return new FindKeys(keys, size);
        }
    }

    private static final Reducer reducer = new Reducer();

    public static Reducer reduce() {
        return reducer;
    }
}