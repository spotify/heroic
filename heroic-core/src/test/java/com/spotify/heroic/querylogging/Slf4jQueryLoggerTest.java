package com.spotify.heroic.querylogging;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.spotify.heroic.Query;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryMetrics;
import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ResultLimits;
import java.util.ArrayList;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class Slf4jQueryLoggerTest {
    @Mock
    public Consumer<String> logger;

    private static final JsonNode EMPTY = JsonNodeFactory.instance.nullNode();

    private ObjectMapper mapper;
    private Slf4jQueryLogger slf4jQueryLogger;

    private QueryContext queryContext;

    @Before
    public void setup() {
        mapper = mock(ObjectMapper.class);
        try {
            when(mapper.writeValueAsString(any())).thenReturn("");
            when(mapper.valueToTree(any())).thenReturn(EMPTY);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        final String component = "<component>";

        slf4jQueryLogger = new Slf4jQueryLogger(logger, mapper, component);

        final Slf4jQueryLoggerFactory queryLoggerFactory = mock(Slf4jQueryLoggerFactory.class);
        when(queryLoggerFactory.create(any())).thenReturn(slf4jQueryLogger);

        queryContext = mock(QueryContext.class);
    }

    @Test
    public void testHttpTextQuery() {
        slf4jQueryLogger.logHttpQueryText(queryContext, "");
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testHttpJsonQuery() {
        final QueryMetrics queryMetrics = mock(QueryMetrics.class);
        slf4jQueryLogger.logHttpQueryJson(queryContext, queryMetrics);
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testQuery() {
        final Query query = mock(Query.class);
        slf4jQueryLogger.logQuery(queryContext, query);
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testOutgoingRequestsToShards() {
        final FullQuery.Request request = mock(FullQuery.Request.class);
        slf4jQueryLogger.logOutgoingRequestToShards(queryContext, request);
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testIncomingRequestAtNode() {
        final FullQuery.Request request = mock(FullQuery.Request.class);
        slf4jQueryLogger.logIncomingRequestAtNode(queryContext, request);
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testOutgoingResponseAtNode() {
        final FullQuery response =
            new FullQuery(QueryTrace.of(QueryTrace.identifier("test"), 0L), new ArrayList<>(),
                new ArrayList<>(), Statistics.empty(), ResultLimits.of());
        slf4jQueryLogger.logOutgoingResponseAtNode(queryContext, response);
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testIncomingResponseFromShard() {
        final FullQuery response =
            new FullQuery(QueryTrace.of(QueryTrace.identifier("test"), 0L), new ArrayList<>(),
                new ArrayList<>(), Statistics.empty(), ResultLimits.of());
        slf4jQueryLogger.logIncomingResponseFromShard(queryContext, response);
        verify(logger, times(1)).accept(any(String.class));
    }

    @Test
    public void testFinalResponse() {
        final QueryMetricsResponse queryMetricsResponse = mock(QueryMetricsResponse.class);
        slf4jQueryLogger.logFinalResponse(queryContext, queryMetricsResponse);
        verify(logger, times(1)).accept(any(String.class));
    }
}
