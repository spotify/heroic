package com.spotify.heroic.metrics;

import java.util.Collection;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

public interface Backend extends Lifecycle {
    @Data
    public abstract class YAML {
        private static final double DEFAULT_THRESHOLD = 10.0;
        private static final long DEFAULT_COOLDOWN = 60000;

        /**
         * Disable this backend if too many errors are being reported by it.
         */
        private boolean disableOnFailures = false;

        /**
         * How many errors per minute that are acceptable.
         */
        private double threshold = DEFAULT_THRESHOLD;

        /**
         * The cooldown period in milliseconds.
         */
        private long cooldown = DEFAULT_COOLDOWN;

        public Backend build(String context, MetricBackendReporter reporter)
                throws ValidationException {
            return buildDelegate(context, reporter);
        }

        protected abstract Backend buildDelegate(String context,
                MetricBackendReporter reporter) throws ValidationException;
    }

    /**
     * Execute a single write.
     *
     * @param write
     * @return
     */
    public Callback<WriteResponse> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param timeSerie
     *            Time serie to write to.
     * @param data
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     */
    public Callback<WriteResponse> write(Collection<WriteMetric> writes);

    /**
     * Query for data points that is part of the specified list of rows and
     * range.
     *
     * @param query
     *            The query for fetching data points. The query contains rows
     *            and a specified time range.
     *
     * @return A list of asynchronous data handlers for the resulting data
     *         points. This is suitable to use with GroupQuery. There will be
     *         one query per row.
     *
     * @throws QueryException
     */
    public List<Callback<FetchDataPoints.Result>> query(
            final TimeSerie timeSerie, final DateRange range);

    /**
     * Gets the total number of columns that are in the given rows
     *
     * @param rows
     * @return
     */
    public Callback<Long> getColumnCount(final TimeSerie timeSerie,
            DateRange range);

    @Override
    public boolean isReady();
}
