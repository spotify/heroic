package com.spotify.heroic.shell;


public interface ShellTaskParams {
    public String config();

    public boolean help();

    public String output();

    public String profile();
}