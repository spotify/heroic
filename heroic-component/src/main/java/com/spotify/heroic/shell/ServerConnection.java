package com.spotify.heroic.shell;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import com.spotify.heroic.shell.protocol.Acknowledge;
import com.spotify.heroic.shell.protocol.CommandOutput;
import com.spotify.heroic.shell.protocol.FileClose;
import com.spotify.heroic.shell.protocol.FileFlush;
import com.spotify.heroic.shell.protocol.FileNewInputStream;
import com.spotify.heroic.shell.protocol.FileNewOutputStream;
import com.spotify.heroic.shell.protocol.FileOpened;
import com.spotify.heroic.shell.protocol.FileRead;
import com.spotify.heroic.shell.protocol.FileReadResult;
import com.spotify.heroic.shell.protocol.FileWrite;

import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.StreamSerialWriter;

final class ServerConnection extends ShellConnection implements ShellIO {
    public static final int BUFFER_SIZE = (1 << 16);

    final PrintWriter out;

    public ServerConnection(final SerializerFramework framework, final SerialReader reader, final StreamSerialWriter writer) throws IOException {
        super(framework, reader, writer);

        this.out = new PrintWriter(new Writer() {
            private CharArrayWriter out = new CharArrayWriter(BUFFER_SIZE);

            @Override
            public void close() throws IOException {
            }

            @Override
            public void flush() throws IOException {
                send(new CommandOutput(out.toCharArray()));
                out.reset();
            }

            @Override
            public void write(char[] b, int off, int len) throws IOException {
                out.write(b, off, len);
            }
        });
    }

    @Override
    public InputStream newInputStream(Path path, StandardOpenOption... options) throws IOException {
        final FileOpened result = request(new FileNewInputStream(path.toString(), Arrays.asList(options)),
                FileOpened.class);

        final int handle = result.getHandle();

        /* BufferedInputStream should _never_ use {@link InputStream#read()}, making sure we don't have to implement
         * that method. Reading one byte at-a-time over the network would be a folly */
        return new BufferedInputStream(new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("read(): not implemented");
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                final FileReadResult data = request(new FileRead(handle, len), FileReadResult.class);
                final byte[] bytes = data.getData();
                System.arraycopy(bytes, 0, b, off, bytes.length);
                return bytes.length;
            }

            @Override
            public void close() throws IOException {
                request(new FileClose(handle), Acknowledge.class);
            }
        }, BUFFER_SIZE);

    }

    @Override
    public OutputStream newOutputStream(Path path, StandardOpenOption... options) throws IOException {
        final FileOpened result = request(new FileNewOutputStream(path.toString(), Arrays.asList(options)),
                FileOpened.class);

        final int handle = result.getHandle();

        return new BufferedOutputStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("write(int): not implemented");
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                final byte[] buffer = new byte[len];
                System.arraycopy(b, off, buffer, 0, len);
                request(new FileWrite(handle, buffer), Acknowledge.class);
            }

            @Override
            public void flush() throws IOException {
                request(new FileFlush(handle), Acknowledge.class);
            }

            @Override
            public void close() throws IOException {
                request(new FileClose(handle), Acknowledge.class);
            }
        }, BUFFER_SIZE);
    }

    @Override
    public PrintWriter out() {
        return out;
    }
}