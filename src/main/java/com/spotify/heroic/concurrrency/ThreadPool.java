package com.spotify.heroic.concurrrency;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

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
@ToString(exclude = { "executor", "context" })
public class ThreadPool {
    public static int DEFAULT_THREADS = 20;
    public static int DEFAULT_QUEUE_SIZE = 10000;

    public static ThreadPool create(String name, ThreadPoolReporter reporter,
            Integer threads, Integer queueSize) {
        if (threads == null)
            threads = DEFAULT_THREADS;

        final int size;

        if (queueSize == null) {
            size = DEFAULT_QUEUE_SIZE;
        } else {
            size = queueSize;
        }

        final ThreadPoolExecutor executor = new ThreadPoolExecutor(threads,
                threads, 5000, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(size, false));

        final ThreadPoolReporter.Context context = reporter
                .newThreadPoolContext(name, new ThreadPoolReporterProvider() {
                    @Override
                    public long getQueueSize() {
                        return executor.getQueue().size();
                    }

                    @Override
                    public long getQueueCapacity() {
                        return size - executor.getQueue().size();
                    }

                    @Override
                    public long getActiveThreads() {
                        return executor.getActiveCount();
                    }

                    @Override
                    public long getPoolSize() {
                        return executor.getPoolSize();
                    }

                    @Override
                    public long getCorePoolSize() {
                        return executor.getCorePoolSize();
                    }
                });

        return new ThreadPool(name, executor, context, threads);
    }

    private final String name;
    private final ThreadPoolExecutor executor;
    private final ThreadPoolReporter.Context context;
    @Getter
    private final int threadPoolSize;

    public ExecutorService get() {
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
