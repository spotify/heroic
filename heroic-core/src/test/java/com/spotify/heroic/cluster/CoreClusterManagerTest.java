package com.spotify.heroic.cluster;

import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.scheduler.Scheduler;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CoreClusterManagerTest {
    @Mock
    AsyncFuture<Void> voidFuture;

    @Mock
    AsyncFramework async;

    @Mock
    ClusterDiscovery discovery;

    @Mock
    NodeMetadata localMetadata;

    @Mock
    Map<String, RpcProtocol> protocols;

    @Mock
    Scheduler scheduler;

    @Mock
    HeroicConfiguration options;

    @Mock
    LocalClusterNode local;

    @Mock
    HeroicContext context;

    private CoreClusterManager manager;

    @Before
    public void setUp() {
        final boolean useLocal = true;

        manager = spy(new CoreClusterManager(async, discovery, localMetadata, protocols, scheduler,
            useLocal, options, local, context));
    }

    @Test
    public void basicRefresh() throws Exception {
        doReturn(voidFuture).when(manager).refreshDiscovery(any(String.class));
        assertEquals(voidFuture, manager.refresh());
        verify(manager).refreshDiscovery(any(String.class));
    }
}
