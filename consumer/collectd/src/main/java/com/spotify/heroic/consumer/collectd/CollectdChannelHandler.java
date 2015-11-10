package com.spotify.heroic.consumer.collectd;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.common.GrokProcessor;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.DatagramPacket;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CollectdChannelHandler extends SimpleChannelInboundHandler<DatagramPacket> {
    private final AsyncFramework async;
    private final IngestionGroup ingestion;
    private final Optional<GrokProcessor> hostProcessor;

    private final CollectdTypes types = new CollectdTypes();

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final DatagramPacket msg) throws Exception {
        final Iterator<CollectdSample> samples = CollectdParser.parse(msg.content());

        while (samples.hasNext()) {
            final CollectdSample s = samples.next();

            final Set<Map.Entry<String, String>> base = ImmutableMap.of("host", s.getHost()).entrySet();

            final List<WriteMetric> writes;

            if (hostProcessor.isPresent()) {
                final Map<String, Object> parts = hostProcessor.get().parse(s.getHost());

                final Set<Map.Entry<String, String>> tags = ImmutableSet
                        .copyOf(Iterables.transform(parts.entrySet(), e -> Pair.of(e.getKey(), e.getValue().toString())));

                writes = types.convert(s, Iterables.concat(base, tags));
            } else {
                writes = types.convert(s, base);
            }

            final List<AsyncFuture<WriteResult>> futures = new ArrayList<>();

            for (final WriteMetric w : writes) {
                futures.add(ingestion.write(w));
            }

            async.collectAndDiscard(futures);
        }
    }
}