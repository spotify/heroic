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

package com.spotify.heroic.aggregation.simple;

import com.spotify.heroic.metric.Point;

public class PointPairArea {
    /**
     * Compute the area that two data points form with the X-axis. The area is positive above
     * the X-axis and negative below.
     *
     * @param a first data point
     * @param b second data point
     * @return area
     */
    public static double computeArea(Point a, Point b) {
        if (a.getTimestamp() == b.getTimestamp()) {
            return 0;
        }

        if (a.getTimestamp() > b.getTimestamp()) {
            return computeArea(b, a);
        }

        final double x1 = a.getTimestamp();
        final double x2 = b.getTimestamp();
        final double y1 = a.getValue();
        final double y2 = b.getValue();

        if (sameSign(y1, y2)) {
            final double area = areaPositivePoints(x1, Math.abs(y1), x2, Math.abs(y2));
            return Math.copySign(area, y1);
        } else {
            // Compute two areas: from x1 to interceptsX and from there to x2
            final double interceptsX = computeInterceptsX(x1, x2, y1, y2);

            final double area1 = areaPositivePoints(x1, Math.abs(y1), interceptsX, 0);
            final double area2 = areaPositivePoints(interceptsX, 0, x2, Math.abs(y2));

            return Math.copySign(area1, y1) + Math.copySign(area2, y2);
        }
     }

    private static double computeInterceptsX(double x1, double x2, double y1, double y2) {
        final double slope = (y2 - y1) / (x2 - x1);
        return x1 - (y1 / slope);
    }

    private static double areaPositivePoints(double x1, double y1, double x2, double y2) {
        final double base = x2 - x1;
        final double height = Math.min(y1, y2);
        final double rectangle = base * height;
        final double triangle = (base * (Math.max(y1, y2) - height)) / 2.0D;

        return rectangle + triangle;
    }

    private static boolean sameSign(double a, double b) {
        // We consider 0 and any other value as same sign
        if (a == 0 || b == 0) {
            return true;
        }

        return  (a < 0) == (b < 0);
    }
}
