package com.spotify.heroic.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.model.DateRange;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AbsoluteDateRangeQuery.class, name = "absolute"),
        @JsonSubTypes.Type(value = RelativeDateRangeQuery.class, name = "relative") })
public interface DateRangeQuery {
    DateRange buildDateRange();
}