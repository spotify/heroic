package com.spotify.heroic.metric.astyanax;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public class MetricsRowKey {
    /**
     * This constant represents the maximum row width of the metrics column family. It equals the amount of numbers that
     * can be represented by Integer. Since the column name is the timestamp offset, having an integer as column offset
     * indicates that we can fit about 49 days of data into one row. We do not assume that Integers are 32 bits. This
     * makes it possible to work even if it's not 32 bits.
     */
    public static final long MAX_WIDTH = (long) Integer.MAX_VALUE - (long) Integer.MIN_VALUE + 1;

    private final Series series;
    private final long base;

    public List<DataPoint> buildDataPoints(ColumnList<Integer> result) {
        final List<DataPoint> datapoints = new ArrayList<DataPoint>();

        for (final Column<Integer> column : result) {
            datapoints.add(new DataPoint(MetricsRowKeySerializer.calculateAbsoluteTimestamp(base, column.getName()),
                    column.getDoubleValue()));
        }

        return datapoints;
    }

    @JsonCreator
    public static MetricsRowKey create(@JsonProperty("series") Series series, @JsonProperty("base") Long base) {
        return new MetricsRowKey(series, base);
    }
}
