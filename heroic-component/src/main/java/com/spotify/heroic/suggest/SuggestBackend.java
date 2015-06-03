package com.spotify.heroic.suggest;

import java.util.List;

import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagKeyCount;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;
import com.spotify.heroic.utils.Grouped;
import com.spotify.heroic.utils.Initializing;

import eu.toolchain.async.AsyncFuture;

public interface SuggestBackend extends Grouped, Initializing {
    /**
     * Return a set of suggestions for the most relevant tag values (given the number of tags available).
     */
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(RangeFilter filter, List<String> exclude, int groupLimit);

    /**
     * Return an estimated count of the given tags.
     */
    public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter);

    public AsyncFuture<TagSuggest> tagSuggest(RangeFilter filter, MatchOptions options, String key, String value);

    public AsyncFuture<KeySuggest> keySuggest(RangeFilter filter, MatchOptions options, String key);

    public AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, String key);

    public AsyncFuture<WriteResult> write(Series series, DateRange range);
}