package com.spotify.heroic.shell;

import org.kohsuke.args4j.Option;


public abstract class AbstractShellTaskParams implements TaskParameters {
    @Option(name = "-c", aliases = { "--config" }, usage = "Path to configuration (only used in standalone)", metaVar = "<config>")
    public String config;

    @Option(name = "-h", aliases = { "--help" }, help = true, usage = "Display help")
    public boolean help;

    @Option(name = "-o", aliases = { "--output" }, usage = "Redirect output to the given file", metaVar = "<file|->")
    public String output;

    @Option(name = "-P", aliases = { "--profile" }, usage = "Activate the given heroic profile", metaVar = "<profile>")
    public String profile;

    @Override
    public String config() {
        return config;
    }

    @Override
    public boolean help() {
        return help;
    }

    @Override
    public String output() {
        return output;
    }

    @Override
    public String profile() {
        return profile;
    }
}