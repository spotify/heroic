package com.spotify.heroic.metadata.async;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.metadata.model.FindTags;

public class FindTagsReducer implements Callback.Reducer<FindTags, FindTags> {
    @Override
    public FindTags resolved(Collection<FindTags> results,
            Collection<Exception> errors, Collection<CancelReason> cancelled)
            throws Exception {
        return mergeFindTags(results);
    }

    private FindTags mergeFindTags(Collection<FindTags> results) {
        final Map<String, Set<String>> tags = new HashMap<String, Set<String>>();
        int size = 0;

        for (final FindTags findTags : results) {
            for (Map.Entry<String, Set<String>> entry : findTags.getTags()
                    .entrySet()) {
                Set<String> entries = tags.get(entry.getKey());

                if (entries == null) {
                    entries = new HashSet<String>();
                    tags.put(entry.getKey(), entries);
                }

                entries.addAll(entry.getValue());
            }

            size += findTags.getSize();
        }

        return new FindTags(tags, size);
    }
}