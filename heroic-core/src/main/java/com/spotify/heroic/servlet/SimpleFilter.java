package com.spotify.heroic.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.spotify.heroic.ws.ErrorMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Class that dictates how simple request filtering operations should be done to
 * its subclasses, which then just implement #doFilterImpl and #passesFilter.
 */
public abstract class SimpleFilter implements Filter {
    private static final String CONTENT_TYPE = "application/json; charset=UTF-8";
    private static final int BYTE_ARRAY_SIZE = 4096;
    private final ObjectMapper mapper;

    public SimpleFilter(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void doFilter(
            final ServletRequest request, final ServletResponse response, final FilterChain chain
    ) throws IOException, ServletException {
        if (passesFilter(request)) {
            chain.doFilter(request, response);
        } else {
            var httpResponse = HttpServletResponse.class.cast(response);

            var info = doFilterImpl((HttpServletRequest) request, httpResponse, chain);

            writeResponse(response, httpResponse, info);
        }
    }
    @Override
    public void init(FilterConfig filterConfig) throws ServletException { /* intentionally empty
     */
    }

    @Override
    public void destroy() { /* intentionally empty */ }

    /**
     * Does this request pass this filter's stipulations i.e. is it a "good"
     * request?
     * @param request request to interrogate
     * @return true if it passes
     */
    public abstract boolean passesFilter(ServletRequest request);

    /**
     * This is an example of the Template Method design pattern where the super
     * class "runs the show" and the subclasses act out its directions.
     */
    protected abstract ErrorMessage doFilterImpl(final HttpServletRequest request,
                                                 final HttpServletResponse response,
                                                 final FilterChain chain
    ) throws IOException, ServletException;

    private void writeResponse(ServletResponse response, HttpServletResponse httpResponse,
                           ErrorMessage info) throws IOException {
        httpResponse.setContentType(CONTENT_TYPE);

        try (final ByteArrayOutputStream output = new ByteArrayOutputStream(BYTE_ARRAY_SIZE)) {
            final OutputStreamWriter writer = new OutputStreamWriter(output, Charsets.UTF_8);
            mapper.writeValue(writer, info);
            response.setContentLength(output.size());
            output.writeTo(httpResponse.getOutputStream());
        }
    }
}
