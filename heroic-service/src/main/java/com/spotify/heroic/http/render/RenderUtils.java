package com.spotify.heroic.http.render;

import java.awt.BasicStroke;
import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.spotify.heroic.metric.model.MetricGroup;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.model.DataPoint;

public final class RenderUtils {
    private static final List<Color> COLORS = new ArrayList<>();

    static {
        COLORS.add(Color.BLUE);
    }

    public static JFreeChart createChart(final MetricGroups groups, final String title, Map<String, String> highlight,
            Double threshold, int height) {
        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer(true, true);

        final XYSeriesCollection dataset = new XYSeriesCollection();

        int i = 0;

        for (final MetricGroup group : groups.getGroups()) {
            if (highlight != null && !highlight.equals(group.getGroup())) {
                continue;
            }

            final XYSeries series = new XYSeries(group.getGroup().toString());

            for (final DataPoint d : group.getDatapoints()) {
                series.add(d.getTimestamp(), d.getValue());
            }

            renderer.setSeriesPaint(i, Color.BLUE);
            renderer.setSeriesShapesVisible(i, false);
            renderer.setSeriesStroke(i, new BasicStroke(2.0f));
            dataset.addSeries(series);

            ++i;
        }

        final JFreeChart chart = ChartFactory.createTimeSeriesChart(title, null, null, dataset, false, false, false);

        chart.setAntiAlias(true);
        chart.setBackgroundPaint(Color.WHITE);

        final XYPlot plot = chart.getXYPlot();

        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.BLACK);
        plot.setRangeGridlinePaint(Color.BLACK);

        if (threshold != null) {
            final ValueMarker marker = new ValueMarker(threshold, Color.RED, new BasicStroke(Math.max(
                    Math.min(height / 20, 6), 1)), Color.RED, null, 0.5f);
            plot.addRangeMarker(marker);
        }

        plot.setRenderer(renderer);

        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());

        return chart;
    }
}
