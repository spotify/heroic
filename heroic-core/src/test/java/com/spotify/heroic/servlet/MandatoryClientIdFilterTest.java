package com.spotify.heroic.servlet;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpInput;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * straightforward tests to show that the passesFilter method is correct and that
 * `chain.doFilter(...)` is only called when the x-client-id header is passed,
 * which proves that only such requests are not rejected.
 */
@RunWith(MockitoJUnitRunner.class)
public class MandatoryClientIdFilterTest {

    private static MandatoryClientIdFilter filter;

    @Mock
    private Request request;

    private HttpServletResponse response;

    @Mock
    private FilterChain chain;

    @Mock
    private HttpChannel channel;

    @Mock
    private HttpInput input;

    @Mock
    private HttpOutput out;

    @BeforeClass
    public static void setUpClass() {
        filter = new MandatoryClientIdFilter();
    }

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        response = new Response(channel, out);
    }

    @Test
    public void doNoHeaderTest() throws IOException, ServletException {
        Mockito
                .doReturn(null)
                .when(request)
                .getHeader(MandatoryClientIdFilter.X_CLIENT_ID_HEADER_NAME);

        filter.doFilter(request, response, chain);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        verify(chain, never()).doFilter(same(request), same(response));
    }

    @Test
    public void doEmptyStringHeaderTest() throws IOException, ServletException {
        Mockito
                .doReturn("")
                .when(request)
                .getHeader(MandatoryClientIdFilter.X_CLIENT_ID_HEADER_NAME);

        filter.doFilter(request, response, chain);

        assertEquals(Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        verify(chain, never()).doFilter(same(request), same(response));
    }

    @Test
    public void doCorrectHeaderTest() throws IOException, ServletException {
        Mockito
                .doReturn("fred")
                .when(request)
                .getHeader(MandatoryClientIdFilter.X_CLIENT_ID_HEADER_NAME);

        filter.doFilter(request, response, chain);

        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        verify(chain, Mockito.times(1)).doFilter(same(request), same(response));
    }
}
