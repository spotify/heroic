package com.spotify.heroic.http;

import com.google.inject.Inject;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.http.cluster.ClusterResource;
import com.spotify.heroic.http.metadata.MetadataResource;
import com.spotify.heroic.http.metadata.MetadataResourceModule;
import com.spotify.heroic.http.metrics.MetricsResource;
import com.spotify.heroic.http.parser.ParserResource;
import com.spotify.heroic.http.query.QueryResource;
import com.spotify.heroic.http.render.RenderResource;
import com.spotify.heroic.http.status.StatusResource;
import com.spotify.heroic.http.utils.UtilsResource;
import com.spotify.heroic.http.write.WriteResource;

public class Entry implements HeroicModule {
    @Inject
    private HeroicConfigurationContext config;

    @Override
    public void setup() {
        config.resource(HeroicResource.class);
        config.resource(WriteResource.class);
        config.resource(UtilsResource.class);
        config.resource(StatusResource.class);
        config.resource(RenderResource.class);
        config.resource(QueryResource.class);
        config.resource(MetadataResource.class);
        config.module(new MetadataResourceModule());
        config.resource(ClusterResource.class);
        config.resource(MetricsResource.class);
        config.resource(ParserResource.class);

        config.resource(ErrorMapper.class);
        config.resource(ParseExceptionMapper.class);
        config.resource(CustomExceptionMapper.class);
        config.resource(UnrecognizedPropertyExceptionMapper.class);
    }
}
