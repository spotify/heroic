package com.spotify.heroic.metric.datastax;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DatastaxAuthenticationTest {
    @Mock
    private Cluster.Builder builder;

    @Test
    public void testNone() {
        final DatastaxAuthentication a = new DatastaxAuthentication.None();
        a.accept(builder);
        verify(builder, never()).withAuthProvider(any(AuthProvider.class));
    }

    @Test
    public void testPlain() {
        final DatastaxAuthentication a =
            new DatastaxAuthentication.Plain(Optional.of("foo"), Optional.of("bar"));
        a.accept(builder);
        verify(builder).withAuthProvider(any(PlainTextAuthProvider.class));
    }
}
