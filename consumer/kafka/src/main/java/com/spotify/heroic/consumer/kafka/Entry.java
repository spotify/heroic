package com.spotify.heroic.consumer.kafka;

import javax.inject.Inject;

import com.spotify.heroic.ConfigurationContext;
import com.spotify.heroic.HeroicEntryPoint;

public class Entry implements HeroicEntryPoint {
    @Inject
    private ConfigurationContext configurationContext;

    @Override
    public void setup() {
        configurationContext.registerType("kafka", KafkaConsumerModule.class);
    }
}
