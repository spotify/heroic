package com.spotify.heroic.filter;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.spotify.heroic.filter.json.FilterDeserializer;
import com.spotify.heroic.filter.json.FilterSerializer;

@JsonDeserialize(using = FilterDeserializer.class)
@JsonSerialize(using = FilterSerializer.class)
public interface Filter {
    public Filter optimize();

    public String operator();
}
