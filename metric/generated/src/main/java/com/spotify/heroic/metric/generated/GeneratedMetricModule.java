package com.spotify.heroic.metric.generated;

import java.util.Set;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.generated.generator.SineGeneratorModule;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.utils.GroupedUtils;

@Data
public final class GeneratedMetricModule implements MetricModule {
    public static final String DEFAULT_GROUP = "generated";

    private final String id;
    private final Set<String> groups;
    private final GeneratorModule generatorModule;

    @JsonCreator
    public GeneratedMetricModule(@JsonProperty("id") String id, @JsonProperty("group") String group,
            @JsonProperty("groups") Set<String> groups, @JsonProperty("generator") GeneratorModule generatorModule) {
        this.id = id;
        this.groups = GroupedUtils.groups(group, groups, DEFAULT_GROUP);
        this.generatorModule = Optional.fromNullable(generatorModule).or(SineGeneratorModule.defaultSupplier());
    }

    @Override
    public PrivateModule module(final Key<MetricBackend> key, final String id) {
        return new PrivateModule() {
            @Provides
            @Singleton
            public MetricBackendReporter reporter(LocalMetricManagerReporter reporter) {
                return reporter.newBackend(id);
            }

            @Provides
            @Singleton
            @Named("groups")
            public Set<String> groups() {
                return groups;
            }

            @Override
            protected void configure() {
                install(generatorModule.module());
                bind(key).to(GeneratedBackend.class).in(Scopes.SINGLETON);
                expose(key);
            }
        };
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String buildId(int i) {
        return String.format("generated#%d", i);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String group;
        private Set<String> groups;
        private GeneratorModule generatorModule;

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder group(String group) {
            this.group = group;
            return this;
        }

        public Builder groups(Set<String> groups) {
            this.groups = groups;
            return this;
        }

        public Builder generatorModule(GeneratorModule generatorModule) {
            this.generatorModule = generatorModule;
            return this;
        }

        public GeneratedMetricModule build() {
            return new GeneratedMetricModule(id, group, groups, generatorModule);
        }
    }
}