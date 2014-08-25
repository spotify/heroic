package com.spotify.heroic;

import javax.inject.Inject;

import lombok.extern.slf4j.Slf4j;

import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.media.sse.SseFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.spotify.heroic.http.HeroicResource;

@Slf4j
public class WebApp extends ResourceConfig {
    @Inject
    public WebApp(ServiceLocator serviceLocator) {
        log.info("Setting up Web Application");

        register(SseFeature.class);

        // Resources.
        packages(HeroicResource.class.getPackage().getName());
        packages(JacksonJsonProvider.class.getPackage().getName());

        GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);

        final GuiceIntoHK2Bridge bridge = serviceLocator
                .getService(GuiceIntoHK2Bridge.class);

        bridge.bridgeGuiceInjector(Main.injector);

    }
}