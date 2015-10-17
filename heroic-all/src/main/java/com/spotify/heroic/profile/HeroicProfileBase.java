package com.spotify.heroic.profile;

import java.util.List;

import org.elasticsearch.common.collect.ImmutableList;

import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.HeroicProfile;

public abstract class HeroicProfileBase implements HeroicProfile {
    @Override
    public List<ParameterSpecification> options() {
        return ImmutableList.of();
    }
}