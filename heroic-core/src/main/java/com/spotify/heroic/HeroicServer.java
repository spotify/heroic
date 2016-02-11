/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.http.CorsResponseFilter;
import com.spotify.heroic.jetty.JettyJSONErrorHandler;
import com.spotify.heroic.jetty.JettyServerConnector;
import com.spotify.heroic.servlet.ShutdownFilter;

import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString(of = {"address"})
public class HeroicServer implements LifeCycle {
    public static final String DEFAULT_CORS_ALLOW_ORIGIN = "*";

    @Inject
    @Named("bindAddress")
    private InetSocketAddress address;

    @Inject
    private Injector injector;

    @Inject
    private HeroicConfigurationContext config;

    @Inject
    @Named(MediaType.APPLICATION_JSON)
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Inject
    @Named("enableCors")
    private boolean enableCors;

    @Inject
    @Named("corsAllowOrigin")
    private Optional<String> corsAllowOrigin;

    @Inject
    private List<JettyServerConnector> connectors;

    @Inject
    @Named("stopping")
    private Supplier<Boolean> stopping;

    private volatile Server server;

    private final Object lock = new Object();

    @Override
    public AsyncFuture<Void> start() {
        final Server newServer = new Server();
        setupConnectors(newServer).forEach(newServer::addConnector);

        try {
            newServer.setHandler(setupHandler());
        } catch (final Exception e) {
            return async.failed(e);
        }

        return async.call(() -> {
            synchronized (lock) {
                if (server != null) {
                    throw new RuntimeException("Server already started");
                }

                newServer.start();
                server = newServer;
            }

            log.info("Started HTTP Server on {}", address);
            return null;
        });
    }

    public int getPort() {
        if (server == null) {
            throw new IllegalStateException("Server is not running");
        }

        return findServerConnector(server).getLocalPort();
    }

    private List<Connector> setupConnectors(final Server server) {
        return ImmutableList
                .copyOf(connectors.stream().map(c -> c.setup(server, address)).iterator());
    }

    private ServerConnector findServerConnector(Server server) {
        final Connector[] connectors = server.getConnectors();

        if (connectors.length == 0) {
            throw new IllegalStateException("server has no connectors");
        }

        for (final Connector c : connectors) {
            if (!(c instanceof ServerConnector)) {
                continue;
            }

            return (ServerConnector) c;
        }

        throw new IllegalStateException("Server has no associated ServerConnector");
    }

    @Override
    public AsyncFuture<Void> stop() {
        return async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final Server s;

                synchronized (lock) {
                    if (server == null) {
                        throw new IllegalStateException("Server has not been started");
                    }

                    s = server;
                    server = null;
                }

                log.info("Stopping http server");
                s.stop();
                s.join();
                return null;
            }
        });
    }

    @Override
    public boolean isReady() {
        return server != null;
    }

    private HandlerCollection setupHandler() throws Exception {
        final ResourceConfig resourceConfig = setupResourceConfig();
        final ServletContainer servlet = new ServletContainer(resourceConfig);

        final ServletHolder jerseyServlet = new ServletHolder(servlet);
        // Initialize and register Jersey ServletContainer

        jerseyServlet.setInitOrder(1);

        // statically provide injector to jersey application.
        final ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        context.addServlet(jerseyServlet, "/*");
        context.addFilter(new FilterHolder(new ShutdownFilter(stopping, mapper)), "/*", null);
        context.setErrorHandler(new JettyJSONErrorHandler(mapper));

        final RequestLogHandler requestLogHandler = new RequestLogHandler();

        requestLogHandler.setRequestLog(new Slf4jRequestLog());

        final RewriteHandler rewrite = new RewriteHandler();
        makeRewriteRules(rewrite);

        final HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] {rewrite, context, requestLogHandler});

        return handlers;
    }

    private void makeRewriteRules(RewriteHandler rewrite) {
        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/metrics");
            rule.setReplacement("/query/metrics");
            rewrite.addRule(rule);
        }

        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/metrics-stream/*");
            rule.setReplacement("/query/metrics-stream");
            rewrite.addRule(rule);
        }

        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/tags");
            rule.setReplacement("/metadata/tags");
            rewrite.addRule(rule);
        }

        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/keys");
            rule.setReplacement("/metadata/keys");
            rewrite.addRule(rule);
        }
    }

    private ResourceConfig setupResourceConfig() throws Exception {
        final ResourceConfig c = new ResourceConfig();

        for (final Class<?> resource : config.getResources()) {
            if (log.isTraceEnabled()) {
                log.trace("Loading resource: {}", resource);
            }

            c.register(setupResource(resource));
        }

        // Resources.
        if (enableCors) {
            c.register(new CorsResponseFilter(corsAllowOrigin.orElse(DEFAULT_CORS_ALLOW_ORIGIN)));
        }

        c.register(new JacksonJsonProvider(mapper), MessageBodyReader.class,
                MessageBodyWriter.class);

        log.info("Loaded {} resource(s)", config.getResources().size());
        return c;
    }

    private Object setupResource(Class<?> resource) {
        final Constructor<?> constructor;

        try {
            constructor = resource.getConstructor();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(resource + ": does not have an empty constructor");
        }

        final Object instance;

        try {
            instance = constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(resource + ": failed to call constructor", e);
        }

        injector.injectMembers(instance);

        return instance;
    }
}
