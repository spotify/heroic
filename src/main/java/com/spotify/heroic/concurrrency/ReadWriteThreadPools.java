package com.spotify.heroic.concurrrency;

import java.util.concurrent.Executor;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.statistics.ThreadPoolReporter;

/**
 * An abstraction for the concept of having separate thread pools dedicated
 * towards reading vs. writing to separate filling one up.
 *
 * @author udoprog
 */
@RequiredArgsConstructor
public class ReadWriteThreadPools {
    @Data
    public static class Config {
        private final int readThreads;
        private final int readQueueSize;
        private final int writeThreads;
        private final int writeQueueSize;

        @JsonCreator
        public static Config create(
                @JsonProperty("readThreads") Integer readThreads,
                @JsonProperty("readQueueSize") Integer readQueueSize,
                @JsonProperty("writeThreads") Integer writeThreads,
                @JsonProperty("writeQueueSize") Integer writeQueueSize) {
            if (readThreads == null)
                readThreads = ThreadPool.DEFAULT_THREADS;

            if (readQueueSize == null)
                readQueueSize = ThreadPool.DEFAULT_QUEUE_SIZE;

            if (writeThreads == null)
                writeThreads = ThreadPool.DEFAULT_THREADS;

            if (writeQueueSize == null)
                writeQueueSize = ThreadPool.DEFAULT_QUEUE_SIZE;

            return new Config(readThreads, readQueueSize, writeThreads,
                    writeQueueSize);
        }

        public static Config createDefault() {
            return create(null, null, null, null);
        }

        public ReadWriteThreadPools construct(ThreadPoolReporter reporter) {
            final ThreadPool read = ThreadPool.create("read", reporter,
                    readThreads, readQueueSize);

            final ThreadPool write = ThreadPool.create("write", reporter,
                    writeThreads, writeQueueSize);

            return new ReadWriteThreadPools(read, write);
        }
    }

    private final ThreadPool read;
    private final ThreadPool write;

    public Executor read() {
        return read.get();
    }

    public Executor write() {
        return write.get();
    }

    public void stop() {
        read.stop();
        write.stop();
    }
}
