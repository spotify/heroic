package com.spotify.heroic.elasticsearch.index;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.model.DateRange;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = RotatingIndexMapping.class, name = "rotating"),
        @JsonSubTypes.Type(value = SingleIndexMapping.class, name = "single") })
public interface IndexMapping {
    public String template();

    public String[] indices(DateRange range) throws NoIndexSelectedException;

    public SearchRequestBuilder search(Client client, DateRange range, String type) throws NoIndexSelectedException;

    public DeleteByQueryRequestBuilder deleteByQuery(Client client, DateRange range, String type)
            throws NoIndexSelectedException;

    public CountRequestBuilder count(Client client, DateRange range, String metadataType)
            throws NoIndexSelectedException;
}