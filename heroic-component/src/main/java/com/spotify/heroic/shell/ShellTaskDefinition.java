package com.spotify.heroic.shell;

import java.util.List;

import com.spotify.heroic.HeroicCoreInstance;

public interface ShellTaskDefinition {
    String name();

    List<String> aliases();

    List<String> names();

    String usage();

    ShellTask setup(HeroicCoreInstance injector) throws Exception;
}