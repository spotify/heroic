package com.spotify.heroic.suggest.elasticsearch;

public class SuggestBackendKVIT extends AbstractElasticsearchSuggestBackendIT {
    protected String backendType() {
        return "kv";
    }
}
