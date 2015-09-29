package com.spotify.heroic.shell;

import java.util.List;

import com.spotify.heroic.HeroicCoreInjector;

public interface ShellTaskDefinition {
    String name();

    List<String> aliases();

    List<String> names();

    String usage();

    ShellTask setup(HeroicCoreInjector core) throws Exception;
}