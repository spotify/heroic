package com.spotify.heroic.concurrrency;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.statistics.NullThreadPoolsReporter;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;
import com.spotify.heroic.statistics.ThreadPoolsReporter;

/**
 * An abstraction for the concept of having separate thread pools dedicated
 * towards reading vs. writing to separate filling one up.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
public class ReadWriteThreadPools {
    public static int DEFAULT_READ_THREADS = 20;
    public static int DEFAULT_READ_QUEUE_SIZE = 10000;
    public static int DEFAULT_WRITE_THREADS = 20;
    public static int DEFAULT_WRITE_QUEUE_SIZE = 10000;

    public static final class Config {
        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readThreads = DEFAULT_READ_THREADS;

        private int readQueueSize = DEFAULT_READ_QUEUE_SIZE;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int writeThreads = DEFAULT_WRITE_THREADS;

        private int writeQueueSize = DEFAULT_WRITE_QUEUE_SIZE;

        private ThreadPoolsReporter reporter = new NullThreadPoolsReporter();

        public Config readThreads(int i) {
            this.readThreads = i;
            return this;
        }

        public Config readQueueSize(int i) {
            this.readQueueSize = i;
            return this;
        }

        public Config writeThreads(int i) {
            this.writeThreads = i;
            return this;
        }

        public Config writeQueueSize(int i) {
            this.writeQueueSize = i;
            return this;
        }

        public Config reporter(ThreadPoolsReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        public ReadWriteThreadPools build() {
            final ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(
                    readThreads, readThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(readQueueSize));

            final ThreadPoolExecutor writeExecutor = new ThreadPoolExecutor(
                    writeThreads, writeThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(writeQueueSize));

            final ThreadPoolsReporter.Context readContext = reporter
                    .newThreadPoolContext("read",
                            new ThreadPoolReporterProvider() {
                        @Override
                        public long getQueueSize() {
                            return readExecutor.getQueue().size();
                        }
                    });

            final ThreadPoolsReporter.Context writeContext = reporter
                    .newThreadPoolContext("write",
                            new ThreadPoolReporterProvider() {
                        @Override
                        public long getQueueSize() {
                            return readExecutor.getQueue().size();
                        }
                    });

            return new ReadWriteThreadPools(readExecutor, writeExecutor,
                    readContext, writeContext);
        }
    }

    public static Config config() {
        return new Config();
    }

    @JsonCreator
    private static ReadWriteThreadPools create(
            @JsonProperty("readThreads") Integer readThreads,
            @JsonProperty("readQueueSize") Integer readQueueSize,
            @JsonProperty("writeThreads") Integer writeThreads,
            @JsonProperty("writeQueueSize") Integer writeQueueSize) {
        if (readThreads == null)
            readThreads = DEFAULT_READ_THREADS;

        if (readQueueSize == null)
            readQueueSize = DEFAULT_READ_QUEUE_SIZE;

        if (writeThreads == null)
            writeThreads = DEFAULT_WRITE_THREADS;

        if (writeQueueSize == null)
            writeQueueSize = DEFAULT_WRITE_QUEUE_SIZE;

        return config().readThreads(readThreads).readQueueSize(readQueueSize)
                .writeThreads(writeThreads).writeQueueSize(writeQueueSize)
                .build();
    }

    private final ThreadPoolExecutor read;
    private final ThreadPoolExecutor write;
    private final ThreadPoolsReporter.Context readContext;
    private final ThreadPoolsReporter.Context writeContext;

    public Executor read() {
        return read;
    }

    public Executor write() {
        return write;
    }

    public void stop() {
        read.shutdown();
        write.shutdown();

        try {
            read.awaitTermination(120, TimeUnit.SECONDS);
            log.debug("Gracefully shut down read executor");
        } catch (final InterruptedException e) {
            final List<?> tasks = read.shutdownNow();
            log.error(
                    "Failed to gracefully stop read executors ({} tasks(s) killed)",
                    tasks.size(), e);
        }

        try {
            write.awaitTermination(120, TimeUnit.SECONDS);
            log.debug("Gracefully shut down write executor");
        } catch (final InterruptedException e) {
            final List<?> tasks = write.shutdownNow();
            log.error(
                    "Failed to gracefully stop write executors ({} tasks(s) killed)",
                    tasks.size(), e);
        }

        readContext.stop();
        writeContext.stop();
    }
}
