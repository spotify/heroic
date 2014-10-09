package com.spotify.heroic.metric.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = SeriesError.class, name = "series"),
        @JsonSubTypes.Type(value = NodeError.class, name = "node") })
public interface RequestError {

}
