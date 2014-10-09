package com.spotify.heroic.filter.json;

import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;
import com.spotify.heroic.filter.TrueFilter;

public final class JsonCommon {
    public static FilterSerialization<MatchTagFilter> MATCH_TAG = new TwoTermsSerialization<MatchTagFilter>(
            MatchTagFilter.BUILDER);

    public static FilterSerialization<StartsWithFilter> STARTS_WITH = new TwoTermsSerialization<StartsWithFilter>(
            StartsWithFilter.BUILDER);

    public static FilterSerialization<RegexFilter> REGEX = new TwoTermsSerialization<RegexFilter>(RegexFilter.BUILDER);

    public static FilterSerialization<HasTagFilter> HAS_TAG = new OneTermSerialization<HasTagFilter, String>(
            HasTagFilter.BUILDER, String.class);

    public static FilterSerialization<MatchKeyFilter> MATCH_KEY = new OneTermSerialization<MatchKeyFilter, String>(
            MatchKeyFilter.BUILDER, String.class);

    public static FilterSerialization<NotFilter> NOT = new OneTermSerialization<NotFilter, Filter>(NotFilter.BUILDER,
            Filter.class);

    public static FilterSerialization<AndFilter> AND = new ManyTermsSerialization<AndFilter>(AndFilter.BUILDER);

    public static FilterSerialization<OrFilter> OR = new ManyTermsSerialization<OrFilter>(OrFilter.BUILDER);

    public static NoTermSerialization<TrueFilter> TRUE = new NoTermSerialization<TrueFilter>(TrueFilter.BUILDER);

    public static NoTermSerialization<FalseFilter> FALSE = new NoTermSerialization<FalseFilter>(FalseFilter.BUILDER);
}