package com.spotify.heroic.concurrrency;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.spotify.heroic.statistics.ThreadPoolReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * An abstraction for the concept of having separate thread pools dedicated towards reading vs. writing to separate
 * filling one up.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
@ToString
public class ReadWriteThreadPools {
    @Data
    public static class Config {
        private final int readThreads;
        private final int readQueueSize;
        private final int writeThreads;
        private final int writeQueueSize;

        @JsonCreator
        public Config(@JsonProperty("readThreads") Integer readThreads,
                @JsonProperty("readQueueSize") Integer readQueueSize,
                @JsonProperty("writeThreads") Integer writeThreads,
                @JsonProperty("writeQueueSize") Integer writeQueueSize) {
            this.readThreads = Optional.fromNullable(readThreads).or(ThreadPool.DEFAULT_THREADS);
            this.readQueueSize = Optional.fromNullable(readQueueSize).or(ThreadPool.DEFAULT_QUEUE_SIZE);
            this.writeThreads = Optional.fromNullable(writeThreads).or(ThreadPool.DEFAULT_THREADS);
            this.writeQueueSize = Optional.fromNullable(writeQueueSize).or(ThreadPool.DEFAULT_QUEUE_SIZE);
        }

        public static Config buildDefault() {
            return new Config(null, null, null, null);
        }

        public static Supplier<Config> provideDefault() {
            return new Supplier<Config>() {
                @Override
                public Config get() {
                    return buildDefault();
                }
            };
        }

        public ReadWriteThreadPools construct(AsyncFramework async, ThreadPoolReporter reporter) {
            final ThreadPool read = ThreadPool.create(async, "read", reporter, readThreads, readQueueSize);

            final ThreadPool write = ThreadPool.create(async, "write", reporter, writeThreads, writeQueueSize);

            return new ReadWriteThreadPools(async, read, write);
        }
    }

    private final AsyncFramework async;
    private final ThreadPool read;
    private final ThreadPool write;

    public ExecutorService read() {
        return read.get();
    }

    public ExecutorService write() {
        return write.get();
    }

    public AsyncFuture<Void> stop() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(read.stop());
        futures.add(write.stop());

        return async.collectAndDiscard(futures);
    }
}
