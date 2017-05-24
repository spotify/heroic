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

import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;
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

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class HeatmapUtil {
    private static final List<Color> COLORS = new ArrayList<>();

    static {
        COLORS.add(Color.BLUE);
    }

    public static BufferedImage createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) {
        final XYLineAndShapeRenderer lineAndShapeRenderer = new XYLineAndShapeRenderer(true, true);
        final DeviationRenderer intervalRenderer = new DeviationRenderer();

        final XYSeriesCollection regularData = new XYSeriesCollection();
        final YIntervalSeriesCollection intervalData = new YIntervalSeriesCollection();

        int lineAndShapeCount = 0;
        int intervalCount = 0;

        for (final ShardedResultGroup resultGroup : groups) {
            final MetricCollection group = resultGroup.getMetrics();

            if (group.getType() == MetricType.POINT) {
                final XYSeries series = new XYSeries(resultGroup.getMetrics().toString());

                final List<Point> data = group.getDataAs(Point.class);

                for (final Point d : data) {
                    series.add(d.getTimestamp(), d.getValue());
                }

                //lineAndShapeRenderer.setSeriesPaint(lineAndShapeCount, Color.BLUE);
                //lineAndShapeRenderer.setSeriesShapesVisible(lineAndShapeCount, false);
                //lineAndShapeRenderer.setSeriesStroke(lineAndShapeCount, new BasicStroke(2.0f));
                //regularData.addSeries(series);
                //++lineAndShapeCount;
            }

            if (group.getType() == MetricType.SPREAD) {
                final YIntervalSeries series =
                    new YIntervalSeries(resultGroup.getMetrics().toString());

                final List<Spread> data = group.getDataAs(Spread.class);

                for (final Spread d : data) {
                    series.add(d.getTimestamp(), d.getSum() / d.getCount(), d.getMin(), d.getMax());
                }

                //intervalRenderer.setSeriesPaint(intervalCount, Color.GREEN);
                //intervalRenderer.setSeriesStroke(intervalCount, new BasicStroke(2.0f));
                //intervalRenderer.setSeriesFillPaint(intervalCount, new Color(200, 255, 200));
                //intervalRenderer.setSeriesShapesVisible(intervalCount, false);
                //intervalData.addSeries(series);
                //++intervalCount;
            }
        }


        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());
        double[][] data = new double[][]{{3,2,3,4,5,6},
                                 {2,3,4,5,6,7},
                                 {3,4,5,6,7,6},
                                 {4,5,6,7,6,5}};

        // Step 1: Create our heat map chart using our data.
        HeatChart map = new HeatChart(data);

        // Step 2: Customise the chart.
        map.setTitle("This is my heat chart title");
        map.setXAxisLabel("X Axis");
        map.setYAxisLabel("Y Axis");

        // Step 3: Output the chart to a file.
        //map.saveToFile(new File("java-heat-chart.png"));

        Image img = map.getChartImage();
        BufferedImage bimage = new BufferedImage(img.getWidth(null), img.getHeight(null), BufferedImage.TYPE_INT_ARGB);

        // Draw the image on to the buffered image
        Graphics2D bGr = bimage.createGraphics();
        bGr.drawImage(img, 0, 0, null);
        bGr.dispose();

        // Return the buffered image
        return bimage;


    }


}
