package com.spotify.heroic.query;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AbsoluteDateRange.class, name = "absolute"),
        @JsonSubTypes.Type(value = RelativeDateRange.class, name = "relative") })
public interface DateRange {
    Date start();

    Date end();

    DateRange roundToInterval(long hint);

}
