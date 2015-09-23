package com.spotify.heroic.metric;

public interface QueryOptions {
    /**
     * Indicates if tracing is enabled.
     *
     * Traced queries include defailts in 
     * @return
     */
    public boolean isTracing();

    static final QueryOptions DEFAULTS = new QueryOptions() {
        @Override
        public boolean isTracing() {
            return false;
        }
    };

    public static QueryOptions defaults() {
        return DEFAULTS;
    }
}