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

package com.spotify.heroic.utils;

import java.util.concurrent.TimeUnit;

public class TimeUtils {
    public static TimeUnit parseUnitName(String unitName, TimeUnit defaultUnit) {
        if (unitName == null)
            return defaultUnit;

        final TimeUnit first = TimeUnit.valueOf(unitName.toUpperCase());

        if (first != null)
            return first;

        return defaultUnit;
    }

    public static long parseSize(Long inputSize, final TimeUnit unit, long defaultValue) {
        final long size;

        if (inputSize == null) {
            size = defaultValue;
        } else {
            size = TimeUnit.MILLISECONDS.convert(inputSize, unit);
        }

        if (size <= 0)
            throw new IllegalArgumentException("size must be a positive value");

        return size;
    }

    public static long parseExtent(Long inputExtent, final TimeUnit unit, final long size) {
        final long extent;

        if (inputExtent == null) {
            extent = size;
        } else {
            extent = TimeUnit.MILLISECONDS.convert(inputExtent, unit);
        }

        if (extent <= 0)
            throw new IllegalArgumentException("extent must be a positive value");

        return extent;
    }
}
