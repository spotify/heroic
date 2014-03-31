package com.spotify.heroic;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.UriBuilder;

import lombok.extern.slf4j.Slf4j;

import org.glassfish.grizzly.http.server.HttpServer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.servlet.GuiceServletContextListener;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.yaml.HeroicConfig;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
public class Main extends GuiceServletContextListener {
    public static final String DEFAULT_CONFIG = "heroic.yml";

    public static Injector injector;

    @Override
    protected Injector getInjector() {
        return injector;
    }

    public static Injector setupInjector(final HeroicConfig config) {
        log.info("Building Guice Injector");

        final List<Module> modules = new ArrayList<Module>();

        final SchedulerModule.Config schedulerConfig = new SchedulerModule.Config();

        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                bind(BackendManager.class).toInstance(
                        config.getBackendManager());
            }
        });
        modules.add(new SchedulerModule(schedulerConfig));

        return Guice.createInjector(modules);
    }

    public static void main(String[] args) {
        final String configPath;

        if (args.length < 1) {
            configPath = DEFAULT_CONFIG;
        } else {
            configPath = args[0];
        }

        final HeroicConfig config;

        try {
            config = HeroicConfig.parse(Paths.get(configPath));
        } catch (ValidationException | IOException e) {
            log.error("Invalid configuration file: " + configPath);
            System.exit(1);
            return;
        }

        if (config == null) {
            log.error("No configuration, shutting down");
            System.exit(1);
            return;
        }

        injector = setupInjector(config);

        final GrizzlyServer grizzlyServer = new GrizzlyServer();
        final HttpServer server;

        final URI baseUri = UriBuilder.fromUri("http://127.0.0.1/").port(8080)
                .build();

        try {
            server = grizzlyServer.start(baseUri);
        } catch (IOException e) {
            log.error("Failed to start grizzly server", e);
            System.exit(1);
            return;
        }

        try {
            System.in.read();
        } catch (IOException e) {
            log.error("Failed to read", e);
        }

        final Scheduler scheduler = injector.getInstance(Scheduler.class);

        log.warn("Shutting down scheduler");

        try {
            scheduler.shutdown(true);
        } catch (SchedulerException e) {
            log.error("Scheduler shutdown failed", e);
        }

        try {
            log.warn("Waiting for server to shutdown");
            server.shutdown().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("Server shutdown failed", e);
        }

        log.warn("Bye Bye!");
        System.exit(0);
    }
}