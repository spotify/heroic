package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = CoreQueryOptions.class, name = "core") })
public interface QueryOptions {
    /**
     * Indicates if tracing is enabled.
     *
     * Traces queries will include a {@link QueryTrace} object that indicates detailed timings of the query.
     *
     * @return {@code true} if tracing is enabled.
     */
    public boolean isTracing();

    public static QueryOptions defaults() {
        return CoreQueryOptions.DEFAULTS;
    }

    static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean tracing = false;

        public Builder tracing(boolean tracing) {
            this.tracing = tracing;
            return this;
        }

        public QueryOptions build() {
            return new CoreQueryOptions(tracing);
        }
    }
}