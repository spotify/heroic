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
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Spread;


import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.Image;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.Vector;

public final class HeatmapUtil {
    private static final List<Color> COLORS = new ArrayList<>();
    public int height;
    public Vector v;
    static {
        COLORS.add(Color.BLUE);
    }

    public static BufferedImage createChart(
        final List<ShardedResultGroup> groups, final String title, Map<String, String> highlight,
        Double threshold, int height
    ) {


        for (final ShardedResultGroup resultGroup : groups) {

            final MetricCollection group = (MetricCollection) resultGroup.getMetrics();
            final SeriesValues series =(SeriesValues) SeriesValues.fromSeries(resultGroup.getSeries().iterator());
            //System.out.print(group);
            height = group.size();
            if (group.getType() == MetricType.POINT) {
                //System.out.print(group.getMetrics().toString());

                final List<Point> data = group.getDataAs(Point.class);
                //System.out.print(group.size());

                Map<String, SortedSet<String>> tags =(Map<String, SortedSet<String>>) series.getTags();
                //double v = pair.getValue();
                //System.out.print(v);

                /**
                for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {



                    final SortedSet<String> values = pair.getValue();
                    System.out.print("values :");
                    System.out.println(values);
                    System.out.print("Key : ");
                    System.out.println(pair.getKey());
                    if (values.size() != 1) {
                        continue;
                    }


                }
                 */
                for (final Point d : data) {
                    double t = (double) d.getTimestamp();

                    double v = (double) d.getValue();
                    System.out.print("timestamp : ");
                    System.out.println(t);
                    System.out.print("value : ");
                    System.out.println(v);
                    //String K = pair.getKey();
                    //System.out.print(K);

                }
                SortedSet<String> f = tags.get("f");
                System.out.print("f : ");
                System.out.print(f);
                SortedSet<String> coor = tags.get("coor");
                System.out.print("coor : ");
                System.out.print(coor);
            }
            System.out.print(group.size());


        }


        // final DateAxis rangeAxis = (DateAxis) plot.getRangeAxis();
        // rangeAxis.setStandardTickUnits(DateAxis.createStandardDateTickUnits());
        double[][] datas = new double[][]{{3,2,3,4,5,6},
                                 {2,3,4,5,6,7},
                                 {3,4,5,6,7,6},
                                 {4,5,6,7,6,5}};

        // Step 1: Create our heat map chart using our data.
        HeatChart map = new HeatChart(datas);

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
