package com.spotify.heroic.shell;

import java.util.List;

public interface TaskParameters {
    public String config();

    public boolean help();

    public String output();

    public List<String> profiles();
}