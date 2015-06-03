package com.spotify.heroic.metric;

import java.util.concurrent.atomic.AtomicLong;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class LimitedFetchQuotaWatcher implements FetchQuotaWatcher {
    private final long dataLimit;

    private final AtomicLong read = new AtomicLong();

    @Override
    public boolean readData(long n) {
        final long r = read.addAndGet(n);
        return r < dataLimit;
    }

    @Override
    public boolean mayReadData() {
        return read.get() < dataLimit;
    }

    @Override
    public int getReadDataQuota() {
        final long left = dataLimit - read.get();

        if (left < 0)
            return 0;

        if (left > Integer.MAX_VALUE)
            throw new IllegalStateException("quota too large");

        return (int) left;
    }

    @Override
    public boolean isQuotaViolated() {
        return !mayReadData();
    }
}