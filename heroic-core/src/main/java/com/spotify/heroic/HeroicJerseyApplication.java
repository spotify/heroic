package com.spotify.heroic;

import javax.inject.Inject;

import lombok.extern.slf4j.Slf4j;

import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.server.ResourceConfig;
import org.jvnet.hk2.guice.bridge.api.GuiceBridge;
import org.jvnet.hk2.guice.bridge.api.GuiceIntoHK2Bridge;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Injector;
import com.spotify.heroic.http.HeroicResource;

/**
 * Contains global jersey configuration.
 *
 * @author udoprog
 */
@Slf4j
public class HeroicJerseyApplication extends ResourceConfig {
    private static Injector injector;

    public static void setInjector(Injector injector) {
        HeroicJerseyApplication.injector = injector;
    }

    @Inject
    public HeroicJerseyApplication(ServiceLocator serviceLocator) {
        if (injector == null) {
            throw new IllegalStateException("No guice injector has been provided with #setInjector");
        }

        log.info("Setting up Web Application");

        // Resources.
        packages(HeroicResource.class.getPackage().getName());
        packages(JacksonJsonProvider.class.getPackage().getName());

        GuiceBridge.getGuiceBridge().initializeGuiceBridge(serviceLocator);

        final GuiceIntoHK2Bridge bridge = serviceLocator.getService(GuiceIntoHK2Bridge.class);

        bridge.bridgeGuiceInjector(injector);
    }
}