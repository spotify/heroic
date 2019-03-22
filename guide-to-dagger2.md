# A guide to Dagger 2

Heroic uses Dagger 2 for Dependency Injection.
Dagger provides several benefits over Guice, primarily it is safer and faster.

The downside is that Heroic used some Guice features which were hard to
replicate in Dagger, the big one being how the `LifeCycle` annotation was
implemented.

## Components

Heroic is organized into several components, each responsible for a certain
part of stage in the running of heroic.

Each core component (loading, early, primary) has a core implementation which
provides the actual dependencies, but also extend them to include core-only
features.

You can find examples of this in `CoreEarlyComponent`, and
`CorePrimaryComopnent`.

### Loading

The `Loading` component is responsible for loading Heroic modules and generally
initializing very early dependencies (thread pools, async framework).

The `Loading` component is implemented in core as `CoreLoadingComponent`.

### Early

The `Early` component is initialized when the Heroic configuration has been
loaded.
It's primary purpose is to provide all dependencies necessary to run the early
bootstrapping phase which has access to the configuration.

The `Early` component is implemented in core as `CoreEarlyComponent`.

### Primary

The `Primary` component provides all other core dependencies, necessary to
create instances of any configured components.
This includes reporter, lifecycle management, and parser.

The `Primary` component is implemented in core as `CoreEarlyComponent`.

### Core

After setting up instances of all configured modules the `Core` component is
configured.

This contains dependencies for _everything_ and is what is typically used when
late injecting using
`<T> T HeroicCoreInstance#inject(Function<CoreComponent, T> injector)`.

The following is an example using `CoreComponent`:

```java
public class Example {
    public void injectThings() {
        final HeroicCoreInstance instance = ...;

        final MyComponent c = instance.inject(c -> {
            return DaggerExample_MyComponent.builder().coreComponent(c).build();
        });

        final MyInjectedThing thing = c.myInjectedThing();
    }

    @Component(dependencies = CoreComponent.class)
    public static interface MyComponent {
        MyInjectedThing myInjectedThing();
    }

    public static class MyInjectedThing {
        private final QueryManager query;

        @Inject
        public MyInjectedThing(final QueryManager query) {
            this.query = query;
        }

        /* other interesting things */
    }
}
```

## LifeCycle in Dagger 2

Each that may interact with lifecycles is reponsible for `exporting` an
implementation of `LifeCycle`.

For backend components, this means implementing one of the following:

* [`LifeCycle MetricModule.Exposed#life()`](/heroic-component/src/main/java/com/spotify/heroic/metric/MetricModule.java)
* [`LifeCycle MetadataModule.Exposed#life()`](/heroic-component/src/main/java/com/spotify/heroic/metadata/MetadataModule.java)
* [`LifeCycle SuggestModule.Exposed#life()`](/heroic-component/src/main/java/com/spotify/heroic/suggest/SuggestModule.java)

The default LifeCycle is simply `LifeCycle#empty()`, which means _do nothing_.

An implementation of a LifeCycle is done by overriding the `life()` parameter
in the module, as shown in the following example.

```java
public class MyMetricModule implements MetricModule {
    ...

    @Override
    public MetricModule.Exposed module(PrimaryComponent primary, MetricModule.Depends depends, String id) {
        return DaggerMyMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(new M())
            .build();
    }

    @PrivateScope
    @Component(modules = M.class, dependencies = {PrimaryComponent.class, MetricModule.Depends.class})
    public static interface C extends MetricModule.Exposed {
        @Override
        MyBackend backend();
    }

    @Module
    public class M {
        @Provides
        @PrivateScope
        LifeCycle life(LifeCycleManager manager, MyBackend backend) {
            return manager.build(backend);
        }
    }
}
```

`LifeCycleManager#build(LifeCycled)` is a convenience method for building a
properly named `LifeCycle`.

All the applications `LifeCycle`s are eventually combined and initialized in
[`HeroicCore`](/heroic-core/src/main/java/com/spotify/heroic/HeroicCore.java).

This causes all `LifeCycle`s to be registered in `LifeCycleRegistry`, which can
be queried.
