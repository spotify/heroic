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

import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import com.spotify.heroic.metric.model.ShardedResultGroup;
import com.spotify.heroic.metric.model.ShardedResultGroups;
import com.spotify.heroic.model.DataPoint;

public final class RenderUtils {
    private static final List<Color> COLORS = new ArrayList<>();

    static {
        COLORS.add(Color.BLUE);
    }

    public static JFreeChart createChart(final ShardedResultGroups groups, final String title,
            Map<String, String> highlight, Double threshold, int height) {
        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer(true, true);

        final XYSeriesCollection dataset = new XYSeriesCollection();

        int i = 0;

        for (final ShardedResultGroup group : groups.getGroups()) {
            if (highlight != null && !highlight.equals(group.getGroup())) {
                continue;
            }

            if (group.getType() != DataPoint.class)
                continue;

            final XYSeries series = new XYSeries(group.getGroup().toString());

            for (final DataPoint d : group.values(DataPoint.class))
                series.add(d.getTimestamp(), d.getValue());

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
