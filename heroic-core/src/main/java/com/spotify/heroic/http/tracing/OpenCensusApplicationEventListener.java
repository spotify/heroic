/*
 * Copyright (c) 2018 Spotify AB.
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

package com.spotify.heroic.http.tracing;

import static io.opencensus.trace.AttributeValue.booleanAttributeValue;
import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static java.text.MessageFormat.format;

import com.spotify.heroic.common.ServiceInfo;
import com.spotify.heroic.tracing.EnvironmentMetadata;
import com.spotify.heroic.tracing.TracingConfig;
import io.opencensus.contrib.http.util.HttpPropagationUtil;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Span;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.SpanContext;
import io.opencensus.trace.Status;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.propagation.SpanContextParseException;
import io.opencensus.trace.propagation.TextFormat;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.model.Invocable;
import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;



class OpenCensusApplicationEventListener implements ApplicationEventListener {
    private final Tracer globalTracer = Tracing.getTracer();
    private final TextFormat textFormat = HttpPropagationUtil.getCloudTraceFormat();
    private TextFormatGetter<ContainerRequest> textFormatGetter = new TextFormatGetter<>();
    private final OpenCensusFeature.Verbosity verbosity;
    private final TracingConfig tracing;
    private final ServiceInfo serviceInfo;
    private static final EnvironmentMetadata environmentMetadata = EnvironmentMetadata.create();

    /**
     * Creates event listener instance with given
     * {@link com.spotify.heroic.http.tracing.OpenCensusFeature.Verbosity}.
     *
     * @param verbosity desired verbosity level
     */
    public OpenCensusApplicationEventListener(
        final OpenCensusFeature.Verbosity verbosity,
        final TracingConfig tracing,
        final ServiceInfo serviceInfo) {
        this.verbosity = verbosity;
        this.tracing = tracing;
        this.serviceInfo = serviceInfo;
    }

    @Override
    public void onEvent(ApplicationEvent event) {
        // we don't care about the server lifecycle
    }

    @Override
    public RequestEventListener onRequest(RequestEvent requestEvent) {
        if (requestEvent.getType() == RequestEvent.Type.START) {
            Span requestSpan = handleRequestStart(requestEvent.getContainerRequest());
            return new OpenCensusRequestEventListener(requestSpan);
        }
        return null;
    }

    private Span handleRequestStart(ContainerRequest request) {

        SpanBuilder spanBuilder;
        String spanName = request.getRequestUri().getPath();

        try {
            final SpanContext spanContext = textFormat.extract(request, textFormatGetter);
            spanBuilder = globalTracer.spanBuilderWithRemoteParent(spanName, spanContext);

        } catch (SpanContextParseException e) {
            spanBuilder = globalTracer.spanBuilder(spanName);
        }

        final Span span = spanBuilder.startSpan();

        span.putAttribute("version", stringAttributeValue(
            serviceInfo.getVersion() + ":" + serviceInfo.getCommit()));
        span.putAttribute("span.kind", stringAttributeValue("server"));
        span.putAttribute("http.method", stringAttributeValue(request.getMethod()));
        span.putAttribute("http.url", stringAttributeValue(
            request.getRequestUri().toASCIIString()));
        span.putAttribute("http.has_request_entity", booleanAttributeValue(request.hasEntity()));
        span.putAttributes(environmentMetadata.toAttributes());

        tracing.getTags().forEach(
            (key, value) -> span.putAttribute(key, stringAttributeValue(value)));

        request.getHeaders().entrySet()
            .stream()
            .filter(entry -> tracing.getRequestHeadersToTags().contains(entry.getKey()))
            .forEach(entry -> {
                final String key = format("http.header.{0}", entry.getKey());
                span.putAttribute(key, stringAttributeValue(entry.getValue().get(0)));
            });

        request.setProperty(OpenCensusFeature.SPAN_CONTEXT_PROPERTY, span);
        span.addAnnotation("Request started.");
        return span;
    }

    class OpenCensusRequestEventListener implements RequestEventListener {
        private Span requestSpan;
        private Span resourceSpan = null;

        OpenCensusRequestEventListener(final Span requestSpan) {
            this.requestSpan = requestSpan;
        }

        @Override
        public void onEvent(RequestEvent event) {

            switch (event.getType()) {
                case MATCHING_START:
                    logVerbose("Resource matching started.");
                    break;

                case LOCATOR_MATCHED:
                    logVerbose(format("Locator matched. Matched locators: {0}",
                        OpenCensusUtils.formatList(
                            event.getUriInfo().getMatchedResourceLocators())));
                    break;

                case SUBRESOURCE_LOCATED:
                    logVerbose(format("Subresource located: {0}",
                        OpenCensusUtils.formatList(event.getUriInfo().getLocatorSubResources())));
                    break;


                case REQUEST_MATCHED:
                    logVerbose(format("Request matched, method: {0}",
                        event.getUriInfo()
                            .getMatchedResourceMethod()
                            .getInvocable()
                            .getDefinitionMethod()));
                    log("Request filtering started.");
                    break;

                case REQUEST_FILTERED:
                    List<ContainerRequestFilter> requestFilters = new ArrayList<>();
                    event.getContainerRequestFilters().forEach(requestFilters::add);

                    logVerbose(format("Request filtering finished, {0} filter(s) applied.",
                        requestFilters.size()));
                    if (requestFilters.size() > 0) {
                        log(format("Applied request filters: {0}.",
                            OpenCensusUtils.formatProviders(requestFilters)));
                    }
                    break;

                case RESOURCE_METHOD_START:
                    final Invocable invocable =
                        event.getUriInfo().getMatchedResourceMethod().getInvocable();
                    logVerbose(format("Resource method {0} started.",
                        invocable.getDefinitionMethod()));

                    final String spanName = invocable.getHandler().getHandlerClass().getName();
                    globalTracer
                        .spanBuilderWithExplicitParent(spanName, requestSpan)
                        .startScopedSpan();
                    resourceSpan = globalTracer.getCurrentSpan();

                    event.getContainerRequest()
                        .setProperty(OpenCensusFeature.SPAN_CONTEXT_PROPERTY, resourceSpan);
                    break;

                case RESOURCE_METHOD_FINISHED:
                    log("Resource method finished.");
                    break;

                case RESP_FILTERS_START:
                    // this is the first event after resource method is guaranteed to have finished,
                    // even for asynchronous processing; resourceSpan will be finished and the span
                    // in the context will be switched back to the resource span before any further
                    // tracing can occur.
                    event.getContainerRequest().setProperty(
                        OpenCensusFeature.SPAN_CONTEXT_PROPERTY, requestSpan);
                    if (resourceSpan != null) {
                        resourceSpan.end();
                    }
                    logVerbose("Response filtering started.");
                    break;

                case RESP_FILTERS_FINISHED:
                    List<ContainerResponseFilter> responseFilters = new ArrayList<>();
                    event.getContainerResponseFilters().forEach(responseFilters::add);
                    logVerbose(format("Response filtering finished, {0} filter(s) applied.",
                        responseFilters.size()));
                    if (responseFilters.size() > 0) {
                        log(format("Applied response filters: {0}.",
                            OpenCensusUtils.formatProviders(responseFilters)));
                    }
                    break;

                case ON_EXCEPTION:
                    if (resourceSpan != null) {
                        resourceSpan.putAttribute("error",
                            booleanAttributeValue(true));
                        resourceSpan.end();
                    }
                    requestSpan.putAttribute("error", booleanAttributeValue(true));
                    logError(event.getException());
                    break;

                case EXCEPTION_MAPPER_FOUND:
                    log(format("Exception mapper found: {0}.",
                        event.getExceptionMapper().getClass().getName()));
                    break;

                case EXCEPTION_MAPPING_FINISHED:
                    log("Exception mapping finished: "
                        + (event.isResponseSuccessfullyMapped()
                           ? "successfully mapped"
                           : "no exception or mapping failed."));

                    break;

                case FINISHED:
                    if (requestSpan != null) {
                        ContainerResponse response = event.getContainerResponse();
                        if (response != null) {
                            int status = response.getStatus();
                            requestSpan.putAttribute("http.status_code",
                                AttributeValue.longAttributeValue(status));
                            requestSpan.putAttribute("http.has_response_entity",
                                booleanAttributeValue(response.hasEntity()));
                            requestSpan.putAttribute("http.response_length",
                                AttributeValue.longAttributeValue(response.getLength()));
                            requestSpan.setStatus(OpenCensusUtils.mapStatusCode(status));

                            if (status >= 400) {
                                requestSpan.putAttribute("error",
                                    booleanAttributeValue(true));
                            }
                        }
                        requestSpan.end();
                    }
                    break;

                default:
                    // This can't happen.
                    break;
            }
        }

        /**
         * Adds a {@link OpenCensusFeature.Verbosity#TRACE}-level log entry into the request span.
         * @param s log message
         */
        private void logVerbose(String s) {
            log(OpenCensusFeature.Verbosity.TRACE, s);
        }

        /**
         * Adds a {@link OpenCensusFeature.Verbosity#INFO}-level log entry into the request span.
         * @param s log message
         */
        private void log(String s) {
            log(OpenCensusFeature.Verbosity.INFO, s);
        }

        /**
         * Adds a log entry with given
         * {@link com.spotify.heroic.http.tracing.OpenCensusFeature.Verbosity}-level into the
         * request span.
         *
         * @param level desired verbosity level
         * @param s log message
         */
        private void log(OpenCensusFeature.Verbosity level, String s) {
            if (level.ordinal() <= verbosity.ordinal()) {
                requestSpan.addAnnotation(s);
            }
        }

        /**
         * Adds an error log into the request span.
         * @param t exception to be logged.
         */
        private void logError(final Throwable t) {
            requestSpan.setStatus(Status.INTERNAL);
            requestSpan.putAttribute("event", stringAttributeValue("error"));
            requestSpan.addAnnotation(t.toString());
        }
    }
}
