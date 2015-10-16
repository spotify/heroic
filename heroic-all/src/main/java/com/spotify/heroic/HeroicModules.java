package com.spotify.heroic;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.profile.BigtableProfile;
import com.spotify.heroic.profile.CassandraProfile;
import com.spotify.heroic.profile.ElasticsearchMetadataProfile;
import com.spotify.heroic.profile.ElasticsearchSuggestProfile;
import com.spotify.heroic.profile.GeneratedProfile;
import com.spotify.heroic.profile.KafkaConsumerProfile;
import com.spotify.heroic.profile.MemoryProfile;

public class HeroicModules {
    // @formatter:off
    public static final List<Class<?>> ALL_MODULES = ImmutableList.<Class<?>>of(
        com.spotify.heroic.metric.astyanax.Entry.class,
        com.spotify.heroic.metric.datastax.Entry.class,
        com.spotify.heroic.metric.generated.Entry.class,
        com.spotify.heroic.metric.bigtable.Entry.class,

        com.spotify.heroic.metadata.elasticsearch.Entry.class,
        com.spotify.heroic.suggest.elasticsearch.Entry.class,
        // com.spotify.heroic.suggest.lucene.Entry.class,

        com.spotify.heroic.cluster.discovery.simple.Entry.class,

        com.spotify.heroic.aggregation.simple.Entry.class,

        com.spotify.heroic.consumer.kafka.Entry.class,

        com.spotify.heroic.aggregationcache.cassandra2.Entry.class,

        com.spotify.heroic.rpc.nativerpc.Entry.class
    );

    public static final Map<String, HeroicProfile> PROFILES = ImmutableMap.<String, HeroicProfile>builder()
        .put("generated", new GeneratedProfile())
        .put("memory", new MemoryProfile())
        .put("cassandra", new CassandraProfile())
        .put("elasticsearch-metadata", new ElasticsearchMetadataProfile())
        .put("elasticsearch-suggest", new ElasticsearchSuggestProfile())
        .put("kafka-consumer", new KafkaConsumerProfile())
        .put("bigtable", new BigtableProfile())
    .build();
    // @formatter:on

    public static void printProfileUsage(final PrintWriter out, final String option) {
        out.println(String.format("Available Profiles (activate with: %s <profile>):", option));

        for (final Map.Entry<String, HeroicProfile> entry : PROFILES.entrySet()) {
            out.println("  " + entry.getKey() + " - " + entry.getValue().description());

            for (final HeroicProfile.Option o : entry.getValue().options()) {
                if (o.getMetavar().isPresent()) {
                    out.println("    " + o.getName() + "=" + o.getMetavar().get() + " - " + o.getDescription());
                } else {
                    out.println("    " + o.getName() + " - " + o.getDescription());
                }
            }
        }

        out.flush();
    }

    public static void printProfileUsage(final PrintStream out, final String option) {
        final PrintWriter o = new PrintWriter(out);

        try {
            printProfileUsage(o, option);
        } finally {
            o.flush();
        }
    }
}