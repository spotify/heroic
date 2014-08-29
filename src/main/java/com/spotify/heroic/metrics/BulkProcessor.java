package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Batcher that expects someone to periodically flush it.
 *
 * @author udoprog
 */
@Slf4j
@RequiredArgsConstructor
public class BulkProcessor<T> {
    public interface Flushable<T> {
        public void flushWrites(List<T> writes) throws Exception;
    }

    private static final int MAX_SIZE = 10000;
    final List<T> buffer = new ArrayList<>(MAX_SIZE);

    private volatile boolean failed = false;
    private volatile boolean stopped = false;

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
    public boolean enqueue(T write) throws InterruptedException,
            BufferEnqueueException {
        if (stopped)
            return false;

        synchronized (this) {
            while (buffer.size() >= MAX_SIZE && !stopped && !failed)
                this.wait();

            if (stopped)
                throw new BufferEnqueueException("stopped");

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
        } catch (final Exception e) {
            log.error("Failed to flush writes", e);
            this.stopped = true;
        }

        buffer.clear();
        this.notifyAll();
    }

    public boolean isStopped() {
        return stopped;
    }

    public synchronized void stop() {
        this.stopped = true;
        this.notifyAll();
    }
}
