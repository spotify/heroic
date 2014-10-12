package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.List;

import com.spotify.heroic.metric.exceptions.BufferEnqueueException;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Batcher that expects someone to periodically flush it.
 *
 * @author udoprog
 */
@Slf4j
@RequiredArgsConstructor
public class MetricBulkProcessor<T> {
    public interface Flushable<T> {
        public void flushWrites(List<T> writes) throws Exception;
    }

    private static final int MAX_SIZE = 100000;
    final List<T> buffer = new ArrayList<>(MAX_SIZE);

    private volatile boolean stopped = false;
    private volatile boolean failing = false;

    private final Flushable<T> flushable;

    /**
     * Queue a write.
     *
     * Blocks if write buffer is full.
     *
     * @param write
     * @throws InterruptedException
     *             If interrupted during the write.
     */
    public boolean enqueue(T write) throws InterruptedException, BufferEnqueueException {
        if (stopped)
            return false;

        synchronized (this) {
            while (buffer.size() >= MAX_SIZE && !stopped && !failing)
                this.wait();

            if (failing)
                throw new BufferEnqueueException("Flushing is currently failing");

            if (stopped)
                throw new BufferEnqueueException("Processor is stopped");

            buffer.add(write);
        }

        return true;
    }

    /**
     * Flush metrics immediately.
     *
     * Should be called periodically.
     */
    public synchronized void flush() {
        if (buffer.isEmpty())
            return;

        log.debug("Flushing {} writes", buffer.size());

        try {
            flushable.flushWrites(buffer);
            this.failing = false;
            buffer.clear();
        } catch (final Exception e) {
            log.error("Failed to flush writes", e);
            this.failing = true;
        }

        this.notifyAll();
    }

    public boolean isStopped() {
        return stopped;
    }

    public synchronized void stop() {
        this.stopped = true;
        flush();
    }
}
