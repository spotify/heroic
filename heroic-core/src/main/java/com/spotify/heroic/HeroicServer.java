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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.base.JsonMappingExceptionMapper;
import com.fasterxml.jackson.jaxrs.base.JsonParseExceptionMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.google.inject.Injector;
import com.spotify.heroic.injection.LifeCycle;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Slf4j
@ToString(of = { "address" })
public class HeroicServer implements LifeCycle {
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

    private volatile Server server;

    private final Object $lock = new Object();

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                synchronized ($lock) {
                    if (server != null) {
                        throw new IllegalStateException("Server has already been started");
                    }

                    final Server server = new Server(address);
                    server.setHandler(setupHandler());

                    final CountDownLatch latch = new CountDownLatch(1);

                    server.addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
                        @Override
                        public void lifeCycleStarted(org.eclipse.jetty.util.component.LifeCycle event) {
                            latch.countDown();
                        }
                    });

                    server.start();
                    latch.await(10, TimeUnit.SECONDS);

                    HeroicServer.this.server = server;

                    log.info("Started HTTP Server on {}", address);
                }

                return null;
            }
        });
    }

    public int getPort() {
        synchronized ($lock) {
            if (server == null) {
                throw new IllegalStateException("Server is not running");
            }

            return findServerConnector(server).getLocalPort();
        }
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
    public AsyncFuture<Void> stop() throws Exception {
        return async.call(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                synchronized ($lock) {
                    if (server == null) {
                        throw new IllegalStateException("Server has not been started");
                    }

                    log.info("Stopping http server");
                    server.stop();
                    server.join();

                    server = null;
                    return null;
                }
            }
        });
    }

    @Override
    public boolean isReady() {
        return server != null;
    }

    private HandlerCollection setupHandler() throws IOException, ExecutionException {
        // statically provide injector to jersey application.
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        final ResourceConfig resourceConfig = setupResourceConfig();
        final ServletContainer servlet = new ServletContainer(resourceConfig);

        final ServletHolder jerseyServlet = new ServletHolder(servlet);
        // Initialize and register Jersey ServletContainer

        jerseyServlet.setInitOrder(1);

        context.addServlet(jerseyServlet, "/*");

        final RequestLogHandler requestLogHandler = new RequestLogHandler();

        requestLogHandler.setRequestLog(new Slf4jRequestLog());

        final RewriteHandler rewrite = new RewriteHandler();
        makeRewriteRules(rewrite);

        final HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { rewrite, context, requestLogHandler });

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

    private ResourceConfig setupResourceConfig() throws ExecutionException {
        final ResourceConfig c = new ResourceConfig();

        for (final Class<?> resource : config.getResources()) {
            log.info("Loading Resource: {}", resource.getCanonicalName());
            c.register(setupResource(resource));
        }

        // Resources.
        c.register(JsonParseExceptionMapper.class);
        c.register(JsonMappingExceptionMapper.class);
        c.register(new JacksonJsonProvider(mapper), MessageBodyReader.class, MessageBodyWriter.class);

        return c;
    }

    private Object setupResource(Class<?> resource) throws ExecutionException {
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
            throw new ExecutionException(resource + ": failed to call constructor", e);
        }

        injector.injectMembers(instance);

        return instance;
    }
}
