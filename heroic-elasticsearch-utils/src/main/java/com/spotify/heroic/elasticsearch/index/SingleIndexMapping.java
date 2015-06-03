package com.spotify.heroic.elasticsearch.index;

import lombok.ToString;

import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.model.DateRange;

@ToString
public class SingleIndexMapping implements IndexMapping {
    public static final String DEFAULT_INDEX = "heroic";

    private final String index;
    private final String[] indices;

    @JsonCreator
    public SingleIndexMapping(@JsonProperty("index") String index) {
        this.index = Optional.fromNullable(index).or(DEFAULT_INDEX);
        this.indices = new String[] { index };
    }

    public static SingleIndexMapping createDefault() {
        return new SingleIndexMapping(null);
    }

    @Override
    public String template() {
        return "*";
    }

    @Override
    public String[] indices(DateRange range) {
        return indices;
    }

    @Override
    public SearchRequestBuilder search(final Client client, DateRange range, final String type) {
        return client.prepareSearch(index).setTypes(type);
    }

    @Override
    public CountRequestBuilder count(final Client client, DateRange range, final String type) {
        return client.prepareCount(index).setTypes(type);
    }

    @Override
    public DeleteByQueryRequestBuilder deleteByQuery(final Client client, DateRange range, final String type) {
        return client.prepareDeleteByQuery(index).setTypes(type);
    }
}