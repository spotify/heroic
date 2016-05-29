package com.spotify.heroic.suggest.elasticsearch;

public class SuggestBackendV1IT extends AbstractElasticsearchSuggestBackendIT {
    protected String backendType() {
        return "v1";
    }
}
