package com.spotify.heroic.metrics;

import java.util.Collection;
import java.util.List;

import lombok.Data;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metrics.model.BackendEntry;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ConfigUtils;
import com.spotify.heroic.yaml.ValidationException;

public interface Backend extends Lifecycle {
    @Data
    public abstract class YAML {
        private static final double DEFAULT_THRESHOLD = 10.0;
        private static final long DEFAULT_COOLDOWN = 60000;

        /**
         * Identifier for this backend, is used for data-migrations.
         */
        private String group = null;

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

        public Backend build(ConfigContext ctx, int index,
                BackendReporter reporter) throws ValidationException {
            final String id = buildId(ctx, index);
            return buildDelegate(id, ctx, reporter);
        }

        private String buildId(ConfigContext ctx, int index)
                throws ValidationException {
            if (this.group == null)
                return defaultGroup();

            return ConfigUtils.notEmpty(ctx.extend("group"), this.group);
        }

        protected abstract String defaultGroup();

        protected abstract Backend buildDelegate(final String id,
                final ConfigContext ctx, final BackendReporter reporter)
                throws ValidationException;
    }

    public String getGroup();

    /**
     * Execute a single write.
     *
     * @param write
     * @return
     */
    public Callback<WriteResult> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param series
     *            Time serie to write to.
     * @param data
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     */
    public Callback<WriteResult> write(Collection<WriteMetric> writes);

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
    public List<Callback<FetchDataPoints.Result>> query(final Series series,
            final DateRange range);

    /**
     * Gets the total number of columns that are in the given rows
     *
     * @param rows
     * @return
     */
    public Callback<Long> getColumnCount(final Series series, DateRange range);

    @Override
    public boolean isReady();

    /**
     * List all series directly from the database.
     *
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     */
    public Iterable<BackendEntry> listEntries();
}
