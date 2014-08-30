package com.spotify.heroic.cache;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cache.model.CachePutResult;
import com.spotify.heroic.cache.model.CacheQueryResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.SeriesSlice;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ConfigUtils;
import com.spotify.heroic.yaml.ValidationException;

public interface AggregationCache {
    @Data
    public static final class YAML {
        private AggregationCacheBackend.YAML backend = null;

        public AggregationCache build(ConfigContext ctx,
                AggregationCacheReporter reporter) throws ValidationException {

            final ConfigContext backendContext = ctx.extend("backend");

            final AggregationCacheBackend backend = buildBackend(reporter,
                    backendContext);

            return new AggregationCacheImpl(reporter, backend);
        }

        private AggregationCacheBackend buildBackend(
                AggregationCacheReporter reporter, final ConfigContext ctx)
                        throws ValidationException {
            return ConfigUtils.notNull(ctx, this.backend).build(ctx,
                    reporter.newAggregationCacheBackend(ctx));
        }
    }

    public static final class Null implements AggregationCache {
        private Null() {
        }

        @Override
        public Callback<CacheQueryResult> get(SeriesSlice slice,
                AggregationGroup aggregation) {
            throw new NullPointerException();
        }

        @Override
        public Callback<CachePutResult> put(Series series,
                AggregationGroup aggregation, List<DataPoint> datapoints) {
            throw new NullPointerException();
        }
    }

    public static final AggregationCache NULL = new Null();

    public Callback<CacheQueryResult> get(SeriesSlice slice,
            final AggregationGroup aggregation);

    public Callback<CachePutResult> put(Series series,
            AggregationGroup aggregation, List<DataPoint> datapoints);
}
