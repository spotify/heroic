package com.spotify.heroic.filter.json;

import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.HasTagFilter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.filter.OrFilter;
import com.spotify.heroic.filter.RegexFilter;
import com.spotify.heroic.filter.StartsWithFilter;

public final class JsonCommon {
    public static FilterSerialization<MatchTagFilter> MATCH_TAG = new TwoTermsSerialization<MatchTagFilter>(
            MatchTagFilter.BUILDER);

    public static FilterSerialization<StartsWithFilter> STARTS_WITH = new TwoTermsSerialization<StartsWithFilter>(
            StartsWithFilter.BUILDER);

    public static FilterSerialization<RegexFilter> REGEX = new TwoTermsSerialization<RegexFilter>(
            RegexFilter.BUILDER);

    public static FilterSerialization<HasTagFilter> HAS_TAG = new OneTermSerialization<HasTagFilter>(
            HasTagFilter.BUILDER);

    public static FilterSerialization<MatchKeyFilter> MATCH_KEY = new OneTermSerialization<MatchKeyFilter>(
            MatchKeyFilter.BUILDER);

    public static FilterSerialization<AndFilter> AND = new ManyTermsSerialization<AndFilter>(
            AndFilter.BUILDER);

    public static FilterSerialization<OrFilter> OR = new ManyTermsSerialization<OrFilter>(
            OrFilter.BUILDER);
}