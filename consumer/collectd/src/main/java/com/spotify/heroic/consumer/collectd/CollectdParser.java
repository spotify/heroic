/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.consumer.collectd;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.slf4j.Logger;

public class CollectdParser {
    private static final int HOST = 0x0000;
    private static final int TIME = 0x0001;
    private static final int TIME_HR = 0x0008;
    private static final int PLUGIN = 0x0002;
    private static final int PLUGIN_INSTANCE = 0x0003;
    private static final int TYPE = 0x0004;
    private static final int TYPE_INSTANCE = 0x0005;
    private static final int VALUES = 0x0006;
    private static final int INTERVAL = 0x0007;
    private static final int INTERVAL_HR = 0x0009;
    private static final int MESSAGE = 0x0100;
    private static final int SEVERITY = 0x0101;

    private static final long FACTOR_HR = 1 << 30;

    private static final Charset UTF8 = Charsets.UTF_8;
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(CollectdParser.class);

    public static Iterator<CollectdSample> parse(final ByteBuf frame) {
        if (frame.readableBytes() < 4) {
            throw new RuntimeException("frame to short");
        }

        frame.order(ByteOrder.BIG_ENDIAN);

        return new Iterator<CollectdSample>() {
            private Decoded decoded = new Decoded();

            @Override
            public boolean hasNext() {
                return frame.readableBytes() > 0;
            }

            @Override
            public CollectdSample next() {
                while (true) {
                    final int type = frame.readUnsignedShort();
                    final int size = frame.readUnsignedShort();

                    switch (type) {
                        case HOST:
                            decoded.host = parseString(frame, size);
                            break;
                        case TIME:
                            decoded.time = frame.readLong();
                            break;
                        case TIME_HR:
                            decoded.time = (long) (((double) frame.readLong()) / FACTOR_HR);
                            break;
                        case PLUGIN:
                            decoded.plugin = parseString(frame, size);
                            break;
                        case PLUGIN_INSTANCE:
                            decoded.pluginInstance = parseString(frame, size);
                            break;
                        case TYPE:
                            decoded.type = parseString(frame, size);
                            break;
                        case TYPE_INSTANCE:
                            decoded.typeInstance = parseString(frame, size);
                            break;
                        case INTERVAL:
                            decoded.interval = frame.readLong();
                            break;
                        case INTERVAL_HR:
                            decoded.interval = (long) (((double) frame.readLong()) / FACTOR_HR);
                            break;
                        case MESSAGE:
                            decoded.message = parseString(frame, size);
                            break;
                        case SEVERITY:
                            decoded.severity = frame.readLong();
                            break;
                        case VALUES:
                            return decoded.toSample(parseValues(frame, size));
                        default:
                            log.warn("unhandled type: " + type);
                            break;
                    }
                }
            }
        };
    }

    public static String parseString(final ByteBuf frame, final int size) {
        final byte[] buffer = new byte[size - 5];
        frame.readBytes(buffer);

        if (frame.readByte() != '\0') {
            throw new RuntimeException("expected null byte");
        }

        return new String(buffer, UTF8);
    }

    public static List<CollectdValue> parseValues(final ByteBuf frame, @Deprecated final int size) {
        final int n = frame.readUnsignedShort();

        final List<Integer> types = ImmutableList.copyOf(IntStream.range(0, n).map(
            i -> frame.readByte()).iterator());

        final ImmutableList.Builder<CollectdValue> values = ImmutableList.builder();

        for (final int type : types) {
            CollectdSampleType sampleType = CollectdSampleType.fromValue(type);
            if (sampleType == null) {
                throw new IllegalArgumentException("invalid sample type: " + type);
            }

            switch (sampleType) {
                case COUNTER:
                    final long c = frame.readLong();

                    if (c < 0) {
                        throw new IllegalArgumentException("value too large for signed type");
                    }

                    values.add(new Counter(c));
                    break;
                case GAUGE:
                    frame.order(ByteOrder.LITTLE_ENDIAN);
                    values.add(new Gauge(frame.readDouble()));
                    frame.order(ByteOrder.BIG_ENDIAN);
                    break;
                case DERIVE:
                    values.add(new Derive(frame.readLong()));
                    break;
                case ABSOLUTE:
                    final long a = frame.readLong();

                    if (a < 0) {
                        throw new IllegalArgumentException("value too large for signed type");
                    }

                    values.add(new Absolute(a));
                    break;
                default:
                    throw new IllegalArgumentException("invalid sample type: " + type);
            }
        }

        return values.build();
    }

    public static class Decoded {
        private String host = "";
        private long time;
        private String plugin = "";
        private String pluginInstance = "";
        private String type = "";
        private String typeInstance = "";
        private long interval;
        private String message = "";
        private long severity;

        public CollectdSample toSample(final List<CollectdValue> values) {
            return new CollectdSample(host, time, plugin, pluginInstance, type, typeInstance,
                values, interval, message, severity);
        }
    }
}
