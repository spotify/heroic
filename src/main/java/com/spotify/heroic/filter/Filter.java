package com.spotify.heroic.filter;

import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.filter.json.FilterJsonDeserializer;
import com.spotify.heroic.filter.json.FilterJsonSerializer;
import com.spotify.heroic.filter.json.FilterSerialization;
import com.spotify.heroic.filter.json.ManyTermsSerialization;
import com.spotify.heroic.filter.json.OneTermSerialization;
import com.spotify.heroic.filter.json.TwoTermsSerialization;

@JsonDeserialize(using = FilterJsonDeserializer.class)
@JsonSerialize(using = FilterJsonSerializer.class)
public interface Filter {
    public static FilterSerialization<MatchTagFilter> MATCH_TAG = new TwoTermsSerialization<MatchTagFilter>() {
        @Override
        public MatchTagFilter build(String tag, String value) {
            return new MatchTagFilter(tag, value);
        }
    };

    public static FilterSerialization<StartsWithFilter> STARTS_WITH = new TwoTermsSerialization<StartsWithFilter>() {
        @Override
        public StartsWithFilter build(String tag, String value) {
            return new StartsWithFilter(tag, value);
        }
    };

    public static FilterSerialization<RegexFilter> REGEX = new TwoTermsSerialization<RegexFilter>() {
        @Override
        public RegexFilter build(String tag, String value) {
            return new RegexFilter(tag, value);
        }
    };

    public static FilterSerialization<HasTagFilter> HAS_TAG = new OneTermSerialization<HasTagFilter>() {
        @Override
        protected HasTagFilter build(String first) {
            return new HasTagFilter(first);
        }
    };

    public static FilterSerialization<MatchKeyFilter> MATCH_KEY = new OneTermSerialization<MatchKeyFilter>() {
        @Override
        protected MatchKeyFilter build(String first) {
            return new MatchKeyFilter(first);
        }
    };

    public static FilterSerialization<AndFilter> AND = new ManyTermsSerialization<AndFilter>() {
        @Override
        protected AndFilter build(List<Filter> terms) {
            return new AndFilter(terms);
        }
    };

    public static FilterSerialization<OrFilter> OR = new ManyTermsSerialization<OrFilter>() {
        @Override
        protected OrFilter build(List<Filter> terms) {
            return new OrFilter(terms);
        }
    };

    public Filter optimize();

    public String operator();
}
