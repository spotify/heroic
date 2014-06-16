package com.spotify.heroic.http.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.model.DateRange;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AbsoluteDateRangeRequest.class, name = "absolute"),
        @JsonSubTypes.Type(value = RelativeDateRangeRequest.class, name = "relative") })
public interface DateRangeRequest {
    DateRange buildDateRange();
}