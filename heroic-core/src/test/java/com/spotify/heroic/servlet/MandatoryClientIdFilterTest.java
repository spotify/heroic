package com.spotify.heroic.servlet;

import com.spotify.heroic.common.MandatoryClientIdUtil.RequestInfractionSeverity;
import java.io.IOException;
import java.util.Optional;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MandatoryClientIdFilterTest {

    private MandatoryClientIdFilter filter;

    @Mock ServletRequest request;

    @Mock ServletResponse response;

    @Mock FilterChain chain;

    @Before
    public void setUp() throws Exception {
        filter = new MandatoryClientIdFilter(RequestInfractionSeverity.PERMIT, Optional.empty());
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void doFilterTest() throws IOException, ServletException {
        filter.doFilter(request, response, chain);
    }

    @Test
    public void passesFilterTest() {
    }
}
