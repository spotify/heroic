package com.spotify.heroic.concurrrency;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * An abstraction for the concept of having separate thread pools dedicated
 * towards reading vs. writing to separate filling one up.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@Slf4j
public class ReadWriteThreadPools {
    public static final class Config {
        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int readThreads = 20;

        private int readQueueSize = 40;

        /**
         * Threads dedicated to asynchronous request handling.
         */
        private int writeThreads = 20;

        private int writeQueueSize = 1000;

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

        public ReadWriteThreadPools build() {
            final ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(
                    readThreads, readThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(readQueueSize));

            final ThreadPoolExecutor writeExecutor = new ThreadPoolExecutor(
                    writeThreads, writeThreads, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(writeQueueSize));

            return new ReadWriteThreadPools(readExecutor, writeExecutor);
        }
    }

    public static Config config() {
        return new Config();
    }

    private final ThreadPoolExecutor read;
    private final ThreadPoolExecutor write;

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
            log.info("Gracefully shutdown read executor");
        } catch (InterruptedException e) {
            final List<?> tasks = read.shutdownNow();
            log.error(
                    "Failed to gracefully stop read executors ({} tasks(s) killed)",
                    tasks.size(), e);
        }

        try {
            write.awaitTermination(120, TimeUnit.SECONDS);
            log.info("Gracefully shutdown write executor");
        } catch (InterruptedException e) {
            final List<?> tasks = write.shutdownNow();
            log.error(
                    "Failed to gracefully stop write executors ({} tasks(s) killed)",
                    tasks.size(), e);
        }
    }
}
