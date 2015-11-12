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

package com.spotify.heroic.filter.impl;

import java.util.Iterator;
import java.util.List;

public class FilterComparatorUtils {
    public static boolean isEqual(String a, String b) {
        return stringCompare(a, b) == 0;
    }

    /**
     * Return true of both a and b are non-null, and b is a prefix (but not equal to) a.
     */
    public static boolean prefixedWith(String a, String b) {
        if (a == null || b == null) {
            return false;
        }

        // strictly prefixes only.
        if (a.equals(b)) {
            return false;
        }

        return a.startsWith(b);
    }

    public static int stringCompare(String a, String b) {
        if (a == null) {
            if (b != null) {
                return 1;
            }

            return 0;
        }

        if (b == null) {
            return -1;
        }

        return a.compareTo(b);
    }

    /**
     * Compare two lists of things which extend the same type <T>.
     */
    public static <T extends Comparable<T>> int compareLists(List<T> a, List<T> b) {
        final Iterator<T> left = a.iterator();
        final Iterator<T> right = b.iterator();

        if (left.hasNext()) {
            if (!right.hasNext()) {
                return -1;
            }

            final T l = left.next();
            final T r = right.next();

            final int c = l.compareTo(r);

            if (c != 0) {
                return c;
            }
        }

        if (right.hasNext()) {
            return 1;
        }

        return 0;
    }
}
