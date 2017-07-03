package com.spotify.heroic.requestcondition;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.querylogging.HttpContext;
import com.spotify.heroic.querylogging.QueryContext;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RequestConditionTest {
    @Mock
    public RequestCondition a;

    @Mock
    public RequestCondition b;

    @Mock
    public QueryContext queryContext1;

    @Mock
    public QueryContext queryContext2;

    @Mock
    public HttpContext httpContext1;

    @Mock
    public HttpContext httpContext2;

    @Before
    public void setup() {
        doReturn(false).when(a).matches(queryContext1);
        doReturn(true).when(a).matches(queryContext2);
        doReturn(true).when(b).matches(queryContext1);
        doReturn(true).when(b).matches(queryContext2);

        doReturn(Optional.of("foobar")).when(httpContext1).getClientId();
        doReturn(Optional.empty()).when(httpContext2).getClientId();
        doReturn(Optional.of("foobar")).when(httpContext1).getUserAgent();
        doReturn(Optional.empty()).when(httpContext2).getUserAgent();
        doReturn(Optional.of(httpContext1)).when(queryContext1).getHttpContext();
        doReturn(Optional.of(httpContext2)).when(queryContext2).getHttpContext();
    }

    @Test
    public void testAll() {
        final All all = new All(ImmutableList.of(a, b));
        assertFalse(all.matches(queryContext1));
        assertTrue(all.matches(queryContext2));
        assertTrue(new All(ImmutableList.of()).matches(queryContext1));
    }

    @Test
    public void testAny() {
        final Any any = new Any(ImmutableList.of(a, b));
        assertTrue(any.matches(queryContext1));
        assertTrue(any.matches(queryContext2));
        assertFalse(new Any(ImmutableList.of()).matches(queryContext1));
    }

    @Test
    public void testClientId() {
        final ClientId clientId = new ClientId("foobar");
        assertTrue(clientId.matches(queryContext1));
        assertFalse(clientId.matches(queryContext2));
    }

    @Test
    public void testUserAgent() {
        final UserAgent userAgent = new UserAgent("foobar");
        assertTrue(userAgent.matches(queryContext1));
        assertFalse(userAgent.matches(queryContext2));
    }
}
