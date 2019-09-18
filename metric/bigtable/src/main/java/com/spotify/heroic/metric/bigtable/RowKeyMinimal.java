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

package com.spotify.heroic.metric.bigtable;

import eu.toolchain.serializer.AutoSerialize;
import java.util.SortedMap;

@AutoSerialize
// TODO: Convert to Kotlin data class once @AutoSerialize either works or is removed.
public class RowKeyMinimal {
    private final Series series;
    private final long base;

    public RowKeyMinimal(Series series, long base) {
        this.series = series;
        this.base = base;
    }

    public Series getSeries() {
        return this.series;
    }

    public long getBase() {
        return this.base;
    }

    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof RowKeyMinimal)) {
            return false;
        }
        final RowKeyMinimal other = (RowKeyMinimal) o;
        if (!other.canEqual(this)) {
            return false;
        }
        final Object thisSeries = this.getSeries();
        final Object otherSeries = other.getSeries();
        if (thisSeries == null ? otherSeries != null : !thisSeries.equals(otherSeries)) {
            return false;
        }
        if (this.getBase() != other.getBase()) {
            return false;
        }
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof RowKeyMinimal;
    }

    public int hashCode() {
        final int prime = 59;
        int result = 1;
        final Object thisSeries = this.getSeries();
        result = result * prime + (thisSeries == null ? 43 : thisSeries.hashCode());
        final long thisBase = this.getBase();
        result = result * prime + (int) (thisBase >>> 32 ^ thisBase);
        return result;
    }

    public String toString() {
        return "RowKeyMinimal(series=" + this.getSeries() + ", base=" + this.getBase() + ")";
    }

    @AutoSerialize
    static class Series {
        private final String key;
        private final SortedMap<String, String> tags;

        public Series(String key, SortedMap<String, String> tags) {
            this.key = key;
            this.tags = tags;
        }

        public static Series create(com.spotify.heroic.common.Series series) {
            return new Series(series.getKey(), series.getTags());
        }

        public String getKey() {
            return this.key;
        }

        public SortedMap<String, String> getTags() {
            return this.tags;
        }

        public boolean equals(final Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof Series)) {
                return false;
            }
            final Series other = (Series) o;
            if (!other.canEqual(this)) {
                return false;
            }
            final Object thisKey = this.getKey();
            final Object otherKey = other.getKey();
            if (thisKey == null ? otherKey != null : !thisKey.equals(otherKey)) {
                return false;
            }
            final Object thisTags = this.getTags();
            final Object otherTags = other.getTags();
            if (thisTags == null ? otherTags != null : !thisTags.equals(otherTags)) {
                return false;
            }
            return true;
        }

        protected boolean canEqual(final Object other) {
            return other instanceof Series;
        }

        public int hashCode() {
            final int prime = 59;
            int result = 1;
            final Object thisKey = this.getKey();
            result = result * prime + (thisKey == null ? 43 : thisKey.hashCode());
            final Object thisTags = this.getTags();
            result = result * prime + (thisTags == null ? 43 : thisTags.hashCode());
            return result;
        }

        public String toString() {
            return "RowKeyMinimal.Series(key=" + this.getKey() + ", tags=" + this.getTags() + ")";
        }
    }

    public static RowKeyMinimal create(RowKey rowKey) {
        return new RowKeyMinimal(Series.create(rowKey.getSeries()), rowKey.getBase());
    }
}
