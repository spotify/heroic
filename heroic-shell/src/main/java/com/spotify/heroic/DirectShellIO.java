package com.spotify.heroic;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import com.spotify.heroic.shell.ShellIO;

import lombok.Data;

@Data
public class DirectShellIO implements ShellIO {
    final PrintWriter out;

    public InputStream newInputStream(Path path, StandardOpenOption... options) throws IOException {
        return Files.newInputStream(path, options);
    }

    @Override
    public OutputStream newOutputStream(Path path, StandardOpenOption... options) throws IOException {
        return Files.newOutputStream(path, options);
    }

    @Override
    public PrintWriter out() {
        return out;
    }
}