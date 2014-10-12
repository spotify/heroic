package com.spotify.heroic.metadata.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.Reducer;

@Data
public class FindKeys {
    public static final FindKeys EMPTY = new FindKeys(new HashSet<String>(), 0, 0);

    private final Set<String> keys;
    private final int size;
    private final int duplicates;

    public static class SelfReducer implements Reducer<FindKeys, FindKeys> {
        @Override
        public FindKeys resolved(Collection<FindKeys> results, Collection<CancelReason> cancelled) throws Exception {
            final Set<String> keys = new HashSet<>();
            int size = 0;
            int duplicates = 0;

            for (final FindKeys result : results) {
                for (final String k : result.getKeys()) {
                    if (keys.add(k)) {
                        duplicates += 1;
                    }
                }

                duplicates += result.getDuplicates();
                size += result.getSize();
            }

            return new FindKeys(keys, size, duplicates);
        }
    }

    private static final SelfReducer reducer = new SelfReducer();

    public static Reducer<FindKeys, FindKeys> reduce() {
        return reducer;
    }
}