package com.spotify.heroic.http.render;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
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
        COLORS.add(Color.RED);
        COLORS.add(Color.GREEN);
        COLORS.add(Color.YELLOW);
        COLORS.add(Color.BLUE);
        COLORS.add(Color.PINK);
        COLORS.add(Color.ORANGE);
    }

    public static JFreeChart createChart(final MetricGroups groups, final String title) {
        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer(true, false);

        final XYSeriesCollection dataset = new XYSeriesCollection();

        int i = 0;

        for (final MetricGroup group : groups.getGroups()) {
            final XYSeries series = new XYSeries(group.getGroup().toString());

            for (DataPoint d : group.getDatapoints()) {
                series.add(d.getTimestamp(), d.getValue());
            }

            renderer.setSeriesPaint(i++, COLORS.get(Math.abs(group.getGroup().hashCode()) % COLORS.size()));

            dataset.addSeries(series);
        }

        final JFreeChart chart = ChartFactory.createTimeSeriesChart(title, "Date", "Value", dataset, false, false,
                false);

        chart.setBackgroundPaint(Color.WHITE);

        final XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.BLACK);
        plot.setRangeGridlinePaint(Color.BLACK);

        plot.setRenderer(renderer);

        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());

        return chart;
    }
}
