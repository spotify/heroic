package com.spotify.heroic.concurrrency;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;

/**
 * An abstraction for the concept of having separate thread pools dedicated
 * towards reading vs. writing to separate filling one up.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
public class ThreadPool {
    public static int DEFAULT_THREADS = 20;
    public static int DEFAULT_QUEUE_SIZE = 10000;

    @JsonCreator
    public static ThreadPool create(String name, ThreadPoolReporter reporter,
            Integer threads, Integer queueSize) {
        if (threads == null)
            threads = DEFAULT_THREADS;

        if (queueSize == null)
            queueSize = DEFAULT_QUEUE_SIZE;

        final ThreadPoolExecutor executor = new ThreadPoolExecutor(threads,
                threads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(queueSize));

        final ThreadPoolReporter.Context context = reporter
                .newThreadPoolContext(name, new ThreadPoolReporterProvider() {
                    @Override
                    public long getQueueSize() {
                        return executor.getQueue().size();
                    }
                });

        return new ThreadPool(executor, context);
    }

    private final ThreadPoolExecutor executor;
    private final ThreadPoolReporter.Context context;

    public Executor get() {
        return executor;
    }

    public void stop() {
        executor.shutdown();

        try {
            executor.awaitTermination(120, TimeUnit.SECONDS);
            log.debug("Gracefully shut down executor");
        } catch (final InterruptedException e) {
            final List<?> tasks = executor.shutdownNow();
            log.error(
                    "Failed to gracefully stop read executors ({} tasks(s) killed)",
                    tasks.size(), e);
        }

        context.stop();
    }
}
