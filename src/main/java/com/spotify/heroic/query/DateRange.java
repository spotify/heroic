package com.spotify.heroic.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AbsoluteDateRange.class, name = "absolute"),
        @JsonSubTypes.Type(value = RelativeDateRange.class, name = "relative") })
public interface DateRange {
    long start();

    long end();

    long diff();

    DateRange roundToInterval(long hint);
}