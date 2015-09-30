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

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

/**
 * Adapted from the Hadoop Project.
 */
package com.spotify.heroic.aggregation.simple;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;

import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.metric.Point;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * Implementation of the Cormode, Korn, Muthukrishnan, and Srivastava algorithm for streaming calculation of targeted
 * high-percentile epsilon-approximate quantiles.
 * 
 * This is a generalization of the earlier work by Greenwald and Khanna (GK), which essentially allows different error
 * bounds on the targeted quantiles, which allows for far more efficient calculation of high-percentiles.
 * 
 * See: Cormode, Korn, Muthukrishnan, and Srivastava "Effective Computation of Biased Quantiles over Data Streams" in
 * ICDE 2005
 * 
 * Greenwald and Khanna, "Space-efficient online computation of quantile summaries" in SIGMOD 2001
 */
@RequiredArgsConstructor
public class QuantileBucket extends AbstractBucket {
    private final long timestamp;
    private final double quantile;
    private final double error;

    /**
     * Set of active samples.
     */
    private final LinkedList<SampleItem> samples = new LinkedList<SampleItem>();

    /**
     * Total count of samples gathered.
     */
    private long count = 0;

    /**
     * Current batch to insert and the corresponding write index.
     */
    private double[] batch = new double[500];
    private int index = 0;

    /**
     * Add a new data point from the stream.
     * 
     * @param d
     *            data point to add.
     */
    @Override
    public synchronized void updatePoint(Map<String, String> tags, Point d) {
        batch[index] = d.getValue();
        index++;
        count++;

        if (index == batch.length) {
            compact();
        }
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    public synchronized double value() {
        if (index > 0) {
            compact();
        }

        return query(quantile);
    }

    public synchronized int getSampleSize() {
        return samples.size();
    }

    private void compact() {
        insertBatch();
        compressSamples();
    }

    /**
     * Merges items from buffer into the samples array in one pass. This is more efficient than doing an insert on every
     * item.
     */
    private void insertBatch() {
        if (index == 0)
            return;

        Arrays.sort(batch, 0, index);

        // Base case: no samples
        int start = 0;

        if (samples.isEmpty()) {
            samples.add(new SampleItem(batch[0], 0, 1));
            start++;
        }

        final ListIterator<SampleItem> it = samples.listIterator();

        SampleItem prev = it.next();

        for (int i = start; i < index; i++) {
            final double value = batch[i];

            while (it.nextIndex() < samples.size() && prev.value < value)
                prev = it.next();

            // If we found that bigger item, back up so we insert ourselves before it
            if (prev.value > value)
                it.previous();

            // We use different indexes for the edge comparisons, because of the above
            // if statement that adjusts the iterator
            final int delta = calculateDelta(it.previousIndex(), it.nextIndex());

            final SampleItem next = new SampleItem(value, delta, 1);
            it.add(next);
            prev = next;
        }

        index = 0;
    }

    /**
     * Try to remove extraneous items from the set of sampled items. This checks if an item is unnecessary based on the
     * desired error bounds, and merges it with the adjacent item if it is.
     */
    private void compressSamples() {
        if (samples.size() < 2)
            return;

        final ListIterator<SampleItem> it = samples.listIterator();

        SampleItem next = it.next();

        while (it.hasNext()) {
            final SampleItem prev = next;

            next = it.next();

            if (prev.g + next.g + next.delta > allowableError(it.previousIndex()))
                continue;

            next.g += prev.g;

            // Remove prev. it.remove() kills the last thing returned.
            it.previous();
            it.previous();
            it.remove();

            // it.next() is now equal to next, skip it back forward again
            it.next();
        }
    }

    /**
     * Specifies the allowable error for this rank, depending on which quantiles are being targeted.
     * 
     * This is the f(r_i, n) function from the CKMS paper. It's basically how wide the range of this rank can be.
     * 
     * @param rank
     *            the index in the list of samples
     */
    private double allowableError(int rank) {
        int size = samples.size();

        final double error = calculateError(rank, size);
        final double minError = size + 1;

        if (error < minError)
            return error;

        return minError;
    }

    private double calculateError(int rank, int size) {
        if (rank <= quantile * size)
            return (2.0 * this.error * (size - rank)) / (1.0 - quantile);

        return (2.0 * this.error * rank) / quantile;
    }

    private int calculateDelta(int previousIndex, int nextIndex) {
        if (previousIndex == 0 || nextIndex == samples.size())
            return 0;

        return ((int) Math.floor(allowableError(nextIndex))) - 1;
    }

    /**
     * Get the estimated value at the specified quantile.
     * 
     * @param quantile
     *            Queried quantile, e.g. 0.50 or 0.99.
     * @return Estimated value at that quantile.
     */
    private double query(double quantile) {
        if (samples.isEmpty())
            throw new IllegalStateException("no data in estimator");

        int rankMin = 0;
        int desired = (int) (quantile * count);

        ListIterator<SampleItem> it = samples.listIterator();

        SampleItem next = it.next();

        for (int i = 1; i < samples.size(); i++) {
            final SampleItem prev = next;

            next = it.next();

            rankMin += prev.g;

            if (rankMin + next.g + next.delta > desired + (allowableError(i) / 2))
                return prev.value;
        }

        // edge case of wanting max value
        return samples.get(samples.size() - 1).value;
    }

    @AllArgsConstructor
    private static class SampleItem {
        public final double value;
        public final int delta;
        public int g;
    }
}
