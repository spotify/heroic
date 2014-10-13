package com.spotify.heroic;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;

import lombok.extern.slf4j.Slf4j;

import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ResourceConfig;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.spotify.heroic.cluster.httprpc.HttpRpcResource;
import com.spotify.heroic.http.HeroicResource;

/**
 * Contains global jersey configuration.
 *
 * @author udoprog
 */
@Slf4j
public class HeroicJerseyApplication extends ResourceConfig {
    private static Injector injector;
    private static ObjectMapper objectMapper;

    public static void setInjector(Injector injector) {
        HeroicJerseyApplication.injector = injector;

        final ObjectMapper mapper = injector.getInstance(Key.get(ObjectMapper.class,
                Names.named(MediaType.APPLICATION_JSON)));

        if (mapper == null) {
            throw new IllegalStateException("No ObjectMapper available in injector");
        }

        HeroicJerseyApplication.objectMapper = mapper;
    }

    public static ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            throw new NullPointerException("objectMapper");
        }

        return objectMapper;
    }

    @Inject
    public HeroicJerseyApplication(ServiceLocator serviceLocator) {
        if (injector == null) {
            throw new IllegalStateException("No guice injector has been provided with #setInjector");
        }

        log.info("Setting up Web Application");

        GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);

        final GuiceIntoHK2Bridge bridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);

        bridge.bridgeGuiceInjector(injector);

        // Resources.
        packages(HeroicResource.class.getPackage().getName());
        packages(HttpRpcResource.class.getPackage().getName());

        register(JacksonFeature.class);
    }
}