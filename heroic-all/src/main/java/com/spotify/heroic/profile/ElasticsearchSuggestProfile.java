package com.spotify.heroic.profile;

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicParameters;
import com.spotify.heroic.HeroicProfile;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.elasticsearch.ElasticsearchSuggestModule;

public class ElasticsearchSuggestProfile extends HeroicProfileBase {
    private static final Splitter splitter = Splitter.on(',').trimResults();

    @Override
    public HeroicConfig.Builder build(final HeroicParameters params) throws Exception {
        final RotatingIndexMapping.Builder index = RotatingIndexMapping.builder();

        params.get("elasticsearch.pattern").map(index::pattern);

        final ManagedConnectionFactory.Builder connection = ManagedConnectionFactory.builder().index(index.build());

        params.get("elasticsearch.clusterName").map(connection::clusterName);
        params.get("elasticsearch.seeds").map(s -> connection.seeds(ImmutableList.copyOf(splitter.split(s))));

        final ElasticsearchSuggestModule.Builder module = ElasticsearchSuggestModule.builder()
                .connection(connection.build());

        params.get("elasticsearch.type").map(module::backendType);

        return HeroicConfig.builder()
                .suggest(SuggestManagerModule.builder().backends(ImmutableList.<SuggestModule> of(module.build())));
    }

    @Override
    public String description() {
        return "Configures a suggest backend for Elasticsearch";
    }

    static final Joiner arguments = Joiner.on(", ");

    @Override
    public List<Option> options() {
        // @formatter:off
        return ImmutableList.of(
            HeroicProfile.option("elasticsearch.pattern", "Index pattern to use (example: heroic-%s)", "<pattern>"),
            HeroicProfile.option("elasticsearch.clusterName", "Cluster name to connect to", "<string>"),
            HeroicProfile.option("elasticsearch.seeds", "Seeds to connect to", "<host>[:<port][,..]"),
            HeroicProfile.option("elasticsearch.type", "Backend type to use, available types are: " + arguments.join(ElasticsearchSuggestModule.types()), "<type>")
        );
        // @formatter:on
    }
}