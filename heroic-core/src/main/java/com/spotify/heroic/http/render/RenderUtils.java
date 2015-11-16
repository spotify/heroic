/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.http.render;

import java.awt.BasicStroke;
import java.awt.Color;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.DeviationRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.data.xy.YIntervalSeries;
import org.jfree.data.xy.YIntervalSeriesCollection;

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;

public final class RenderUtils {
    private static final List<Color> COLORS = new ArrayList<>();

    static {
        COLORS.add(Color.BLUE);
    }

    public static JFreeChart createChart(final List<ShardedResultGroup> groups, final String title,
            Map<String, String> highlight, Double threshold, int height) {
        final XYLineAndShapeRenderer lineAndShapeRenderer = new XYLineAndShapeRenderer(true, true);
        final DeviationRenderer intervalRenderer = new DeviationRenderer();

        final XYSeriesCollection regularData = new XYSeriesCollection();
        final YIntervalSeriesCollection intervalData = new YIntervalSeriesCollection();

        int lineAndShapeCount = 0;
        int intervalCount = 0;

        for (final ShardedResultGroup resultGroup : groups) {
            final MetricCollection group = resultGroup.getGroup();

            if (group.getType() == MetricType.POINT) {
                final XYSeries series = new XYSeries(resultGroup.getGroup().toString());

                final List<Point> data = group.getDataAs(Point.class);

                for (final Point d : data) {
                    series.add(d.getTimestamp(), d.getValue());
                }

                lineAndShapeRenderer.setSeriesPaint(lineAndShapeCount, Color.BLUE);
                lineAndShapeRenderer.setSeriesShapesVisible(lineAndShapeCount, false);
                lineAndShapeRenderer.setSeriesStroke(lineAndShapeCount, new BasicStroke(2.0f));
                regularData.addSeries(series);
                ++lineAndShapeCount;
            }

            if (group.getType() == MetricType.SPREAD) {
                final YIntervalSeries series =
                        new YIntervalSeries(resultGroup.getGroup().toString());

                final List<Spread> data = group.getDataAs(Spread.class);

                for (final Spread d : data) {
                    series.add(d.getTimestamp(), d.getSum() / d.getCount(), d.getMin(), d.getMax());
                }

                intervalRenderer.setSeriesPaint(intervalCount, Color.GREEN);
                intervalRenderer.setSeriesStroke(intervalCount, new BasicStroke(2.0f));
                intervalRenderer.setSeriesFillPaint(intervalCount, new Color(200, 255, 200));
                intervalRenderer.setSeriesShapesVisible(intervalCount, false);
                intervalData.addSeries(series);
                ++intervalCount;
            }
        }

        final JFreeChart chart = buildChart(title, regularData, intervalData, lineAndShapeRenderer,
                intervalRenderer);

        chart.setAntiAlias(true);
        chart.setBackgroundPaint(Color.WHITE);

        final XYPlot plot = chart.getXYPlot();

        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.BLACK);
        plot.setRangeGridlinePaint(Color.BLACK);

        if (threshold != null) {
            final ValueMarker marker = new ValueMarker(threshold, Color.RED,
                    new BasicStroke(Math.max(Math.min(height / 20, 6), 1)), Color.RED, null, 0.5f);
            plot.addRangeMarker(marker);
        }

        plot.setRenderer(lineAndShapeRenderer);

        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());

        return chart;
    }

    private static JFreeChart buildChart(final String title, final XYDataset lineAndShape,
            final XYDataset interval, final XYItemRenderer lineAndShapeRenderer,
            final XYItemRenderer intervalRenderer) {
        final ValueAxis timeAxis = new DateAxis();
        timeAxis.setLowerMargin(0.02);
        timeAxis.setUpperMargin(0.02);

        final NumberAxis valueAxis = new NumberAxis();
        valueAxis.setAutoRangeIncludesZero(false);

        final XYPlot plot = new XYPlot();

        plot.setDomainAxis(0, timeAxis);
        plot.setRangeAxis(0, valueAxis);

        plot.setDataset(0, lineAndShape);
        plot.setRenderer(0, lineAndShapeRenderer);

        plot.setDomainAxis(1, timeAxis);
        plot.setRangeAxis(1, valueAxis);

        plot.setDataset(1, interval);
        plot.setRenderer(1, intervalRenderer);

        return new JFreeChart(title, JFreeChart.DEFAULT_TITLE_FONT, plot, false);
    }
}
