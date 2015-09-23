package com.spotify.heroic.metric;

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
        return QueryOptionsImpl.DEFAULTS;
    }

    static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean tracing;

        public Builder tracing(boolean tracing) {
            this.tracing = tracing;
            return this;
        }

        public QueryOptions build() {
            return new QueryOptionsImpl(tracing);
        }
    }
}