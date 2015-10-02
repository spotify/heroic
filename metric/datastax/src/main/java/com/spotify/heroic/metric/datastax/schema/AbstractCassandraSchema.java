package com.spotify.heroic.metric.datastax.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.lang3.text.StrSubstitutor;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.spotify.heroic.metric.datastax.Async;
import com.spotify.heroic.metric.datastax.ManagedSetupConnection;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class AbstractCassandraSchema {
    protected final AsyncFramework async;

    protected AsyncFuture<PreparedStatement> prepareTemplate(final Map<String, String> values, Session s, final String path) throws IOException {
        return Async.bind(async, s.prepareAsync(loadTemplate(path, values)));
    }

    protected AsyncFuture<PreparedStatement> prepareAsync(final Map<String, String> values, Session s, final String cql) {
        return Async.bind(async, s.prepareAsync(variables(cql, values)));
    }

    private String loadTemplate(final String path, final Map<String, String> values) throws IOException {
        final String string;
        final ClassLoader loader = ManagedSetupConnection.class.getClassLoader();

        try (final InputStream is = loader.getResourceAsStream(path)) {
            if (is == null)
                throw new IOException("No such resource: " + path);

            string = CharStreams.toString(new InputStreamReader(is, Charsets.UTF_8));
        }

        return new StrSubstitutor(values, "{{", "}}").replace(string);
    }

    private String variables(String cql, Map<String, String> values) {
        return new StrSubstitutor(values, "{{", "}}").replace(cql);
    };
}