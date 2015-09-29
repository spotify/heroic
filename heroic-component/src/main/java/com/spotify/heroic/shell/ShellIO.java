package com.spotify.heroic.shell;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * I/O indirection for shell tasks.
 *
 * If tasks directly open handles, these will be opened in the context of which the task is running. This is most likely
 * a server, which is not what the shell users typically intends.
 *
 * This interface introduces implementation of methods that work correctly, regardless of context.
 *
 * @author udoprog
 */
public interface ShellIO {
    /**
     * Open a file for reading.
     * @param path Path of file to open.
     * @param options Options when opening file.
     * @return An InputStream associated with the open file.
     */
    public InputStream newInputStream(Path path, StandardOpenOption... options) throws IOException;

    /**
     * Open a file for writing.
     * @param path Path of file to open.
     * @param options Options when opening file.
     * @return An OutputStream associated with the open file.
     */
    public OutputStream newOutputStream(Path path, StandardOpenOption... options) throws IOException;

    /**
     * Get the output stream for this task.
     * @return The output strema for this task.
     */
    public PrintWriter out();
}