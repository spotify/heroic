package com.spotify.heroic.shell;

import java.util.List;

import com.spotify.heroic.HeroicCoreInjector;

public interface CoreTaskDefinition {
    String name();

    List<String> aliases();

    List<String> names();

    String usage();

    CoreShellTaskDefinition setup(HeroicCoreInjector core) throws Exception;
}