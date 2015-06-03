package com.spotify.heroic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.inject.Injector;

@Slf4j
@RequiredArgsConstructor
public class HeroicInernalLifeCycleImpl implements HeroicInternalLifeCycle {
    private final List<Runnable> shutdownHooks = new ArrayList<Runnable>();
    private final List<StartupHookRunnable> startupHooks = new LinkedList<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private final Object $lock = new Object();

    private volatile Injector injector;

    private static interface StartupHookRunnable {
        public void run(Injector injector);
    }

    @Override
    public void registerShutdown(final String name, final ShutdownHook hook) {
        if (stopped.get())
            throw new IllegalStateException("lifecycle already stopped");

        synchronized ($lock) {
            if (stopped.get())
                throw new IllegalStateException("lifecycle already stopped");

            shutdownHooks.add(new Runnable() {
                @Override
                public void run() {
                    log.info("Shutting down '{}'", name);
                    runShutdownHook(name, hook);
                }
            });
        }
    }

    @Override
    public void register(final String name, final StartupHook hook) {
        if (started.get())
            throw new IllegalStateException("lifecycle already started");

        synchronized ($lock) {
            if (started.get())
                throw new IllegalStateException("lifecycle already started");

            startupHooks.add(new StartupHookRunnable() {
                @Override
                public void run(final Injector injector) {
                    log.info("Starting up '{}'", name);
                    runStartupHook(name, hook);
                }
            });
        }
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true))
            return;

        final Collection<StartupHookRunnable> hooks;

        synchronized ($lock) {
            hooks = new ArrayList<>(this.startupHooks);
            startupHooks.clear();
        }

        for (StartupHookRunnable hook : hooks) {
            hook.run(injector);
        }
    }

    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true))
            return;

        final Collection<Runnable> hooks;

        synchronized ($lock) {
            hooks = new ArrayList<>(this.shutdownHooks);
            startupHooks.clear();
        }

        for (Runnable hook : hooks) {
            hook.run();
        }
    }

    private void runShutdownHook(final String name, final ShutdownHook hook) {
        try {
            hook.onShutdown();
        } catch (final Exception e) {
            log.error("Failed to shut down '{}'", name, e);
        }
    }

    private void runStartupHook(final String name, final StartupHook hook) {
        final Context context = new Context() {
            @Override
            public void registerShutdown(ShutdownHook hook) {
                HeroicInernalLifeCycleImpl.this.registerShutdown(name, hook);
            }
        };

        try {
            hook.onStartup(context);
        } catch (Exception e) {
            log.error("Failed to start up '{}'", name, e);
        }
    }
}
