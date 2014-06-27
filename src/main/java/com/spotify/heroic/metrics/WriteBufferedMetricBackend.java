package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.injection.Delegator;
import com.spotify.heroic.metrics.model.FetchDataPoints.Result;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteEntry;
import com.spotify.heroic.model.WriteResponse;

@RequiredArgsConstructor
public class WriteBufferedMetricBackend implements MetricBackend,
        Delegator<MetricBackend> {
    private ConcurrentHashMap<TimeSerie, Queue<DataPoint>> buffer = new ConcurrentHashMap<TimeSerie, Queue<DataPoint>>();
    private volatile Callback<WriteResponse> next = new ConcurrentCallback<WriteResponse>();

    private final MetricBackend delegate;

    /**
     * Use a read-write-lock to allow for multiple processes buffering up data,
     * but cause them to halt when the buffer is being acquired by the flushing
     * process.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void flush() {
        final Lock writeLock = lock.writeLock();
        writeLock.lock();

        final Map<TimeSerie, Queue<DataPoint>> buffer;
        final Callback<WriteResponse> callback;

        try {
            callback = this.next;
            buffer = this.buffer;
            this.next = new ConcurrentCallback<WriteResponse>();
            this.buffer = new ConcurrentHashMap<TimeSerie, Queue<DataPoint>>();
        } finally {
            writeLock.unlock();
        }

        final List<WriteEntry> writes = new ArrayList<WriteEntry>(buffer.size());

        for (Map.Entry<TimeSerie, Queue<DataPoint>> entry : buffer.entrySet()) {
            writes.add(new WriteEntry(entry.getKey(), entry.getValue()));
        }

        callback.register(this.delegate.write(writes));
    }

    @Override
    public void start() throws Exception {
        delegate.start();
    }

    @Override
    public void stop() throws Exception {
        delegate.stop();
    }

    @Override
    public MetricBackend delegate() {
        return delegate;
    }

    @Override
    public TimeSerie getPartition() {
        return delegate().getPartition();
    }

    @Override
    public boolean matchesPartition(TimeSerie timeSerie) {
        return delegate().matchesPartition(timeSerie);
    }

    @Override
    public Callback<WriteResponse> write(Collection<WriteEntry> writes) {
        final Lock readLock = this.lock.readLock();
        readLock.lock();

        try {
            for (final WriteEntry write : writes) {
                Queue<DataPoint> data = buffer.get(write.getTimeSerie());

                if (data == null) {
                    // small amount of wasted resources.
                    data = buffer.putIfAbsent(write.getTimeSerie(),
                            new ConcurrentLinkedQueue<DataPoint>());
                }

                data.addAll(write.getData());
            }

            return next;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Callback<WriteResponse> write(WriteEntry write) {
        final Lock readLock = this.lock.readLock();
        readLock.lock();

        try {
            Queue<DataPoint> data = buffer.get(write.getTimeSerie());

            if (data == null) {
                // small amount of wasted resources.
                data = buffer.putIfAbsent(write.getTimeSerie(),
                        new ConcurrentLinkedQueue<DataPoint>());
            }

            data.addAll(write.getData());
            return next;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public List<Callback<Result>> query(TimeSerie timeSerie, DateRange range) {
        return delegate.query(timeSerie, range);
    }

    @Override
    public Callback<Set<TimeSerie>> getAllTimeSeries() {
        return delegate.getAllTimeSeries();
    }

    @Override
    public Callback<Long> getColumnCount(TimeSerie timeSerie, DateRange range) {
        return delegate.getColumnCount(timeSerie, range);
    }

    @Override
    public boolean isReady() {
        return delegate.isReady();
    }
}
