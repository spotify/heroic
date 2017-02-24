package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

public class MinMaxSlidingTimeReservoir implements Reservoir {
    // max spins until calling Thread.yield()
    private static final int MAX_SPINS = 4;

    private final ConcurrentSkipListMap<Long, AtomicReference<MinMaxEntry>> measurements;
    private final int size;
    private final Clock clock;
    private final long step;
    private final Reservoir delegate;

    /**
     * Build a new reservoir.
     *
     * @param clock Clock to use as a time source
     * @param size Number of buckets to maintain
     * @param step Step between each bucket
     * @param delegate Delegate reservoir that min/max is being corrected for.
     */
    public MinMaxSlidingTimeReservoir(
        final Clock clock, final int size, final long step, final Reservoir delegate
    ) {
        this.measurements = new ConcurrentSkipListMap<>();
        this.clock = clock;
        this.size = size;
        this.step = step;
        this.delegate = delegate;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void update(long value) {
        trimIfNeeded();

        final long offset = clock.getTick() / step;

        final AtomicReference<MinMaxEntry> reference = measurements.computeIfAbsent(offset,
            o -> new AtomicReference<>(new MinMaxEntry(value, value)));

        int spins = 0;

        while (true) {
            final MinMaxEntry old = reference.get();

            if (old.min <= value && value <= old.max) {
                break;
            }

            final MinMaxEntry newEntry =
                new MinMaxEntry(Math.min(old.min, value), Math.max(old.max, value));

            if (reference.compareAndSet(old, newEntry)) {
                break;
            }

            if (spins++ >= MAX_SPINS) {
                Thread.yield();
                spins = 0;
            }
        }

        delegate.update(value);
    }

    @Override
    public Snapshot getSnapshot() {
        trimIfNeeded();

        final long first = calculateFirstBucket();

        final Optional<MinMaxEntry> value = measurements
            .tailMap(first)
            .values()
            .stream()
            .map(AtomicReference::get)
            .reduce((a, b) -> new MinMaxEntry(Math.min(a.min, b.min), Math.max(a.max, b.max)));

        final Snapshot snapshot = delegate.getSnapshot();

        return value.map(minMax -> {
            final long[] values = snapshot.getValues();

            if (values.length > 0) {
                values[0] = Math.min(minMax.min, values[0]);
                values[values.length - 1] = Math.max(minMax.max, values[values.length - 1]);
            }

            return new Snapshot(values);
        }).orElse(snapshot);
    }

    /**
     * Trim min-max entries before the first bucket.
     */
    private void trimIfNeeded() {
        final long first = calculateFirstBucket();

        final Map.Entry<Long, AtomicReference<MinMaxEntry>> firstEntry =
            measurements.firstEntry();

        if (firstEntry != null && firstEntry.getKey() <= first) {
            // removes all entries older than first
            measurements.headMap(first, true).clear();
        }
    }

    /**
     * Calculate the first possible bucket that is within the current time interval.
     */
    long calculateFirstBucket() {
        return (clock.getTick() - (step * size)) / step;
    }

    @ToString
    @RequiredArgsConstructor
    private static class MinMaxEntry {
        private final long min;
        private final long max;
    }
}
