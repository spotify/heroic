package com.spotify.heroic;

import com.google.common.base.Throwables;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.TinyAsync;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;

public abstract class AbstractSingleNodeIT {
    protected final ExecutorService executor = Executors.newSingleThreadExecutor();
    protected final TinyAsync async = TinyAsync.builder().executor(executor).build();

    protected HeroicCoreInstance instance;

    protected AsyncFuture<Void> prepareEnvironment() {
        return async.resolved(null);
    }

    protected HeroicConfig.Builder setupConfig() {
        return HeroicConfig.builder();
    }

    @Before
    public final void abstractSetup() throws Exception {
        instance = setupCore();
        instance.start().lazyTransform(ignore -> prepareEnvironment()).get(10, TimeUnit.SECONDS);
    }

    @After
    public final void abstractTeardown() throws Exception {
        instance.shutdown().get(10, TimeUnit.SECONDS);
    }

    private HeroicCoreInstance setupCore() {
        try {
            return setupCoreThrowing();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private HeroicCoreInstance setupCoreThrowing() throws Exception {
        return HeroicCore
            .builder()
            .setupShellServer(false)
            .setupService(false)
            .oneshot(true)
            .executor(executor)
            .configFragment(setupConfig())
            .modules(HeroicModules.ALL_MODULES)
            .build()
            .newInstance();
    }
}
