package gratis;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * A small RESP client for black-box tests. It reads exactly one reply at a time and returns it as the raw text the
 * server sent, so a test can compare the wire format byte for byte, with no sleeps.
 */
final class RespClient implements AutoCloseable {
    private static final int TIMEOUT_MS = 5_000;

    private final Socket socket;
    private final OutputStream out;
    private final InputStream in;

    RespClient(int port) throws IOException {
        socket = new Socket("127.0.0.1", port);
        socket.setSoTimeout(TIMEOUT_MS);
        socket.setTcpNoDelay(true);
        out = socket.getOutputStream();
        in = new BufferedInputStream(socket.getInputStream());
    }

    static byte[] encode(String... args) {
        StringBuilder command = new StringBuilder("*").append(args.length).append("\r\n");
        for (String arg : args) {
            command.append('$').append(arg.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(arg)
                    .append("\r\n");
        }
        return command.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** Sends one command and returns its reply. */
    String call(String... args) throws IOException {
        send(encode(args));
        return reply();
    }

    void send(byte[] bytes) throws IOException {
        out.write(bytes);
        out.flush();
    }

    /** Reads one complete reply, including every element of an array. */
    String reply() throws IOException {
        ByteArrayOutputStream raw = new ByteArrayOutputStream();
        readReply(raw);
        return raw.toString(StandardCharsets.UTF_8);
    }

    /** True if the server sends nothing on this connection for {@code wait}. */
    boolean silentFor(Duration wait) throws IOException {
        socket.setSoTimeout((int) wait.toMillis());
        try {
            return in.read() == -1;
        } catch (SocketTimeoutException quiet) {
            return true;
        } finally {
            socket.setSoTimeout(TIMEOUT_MS);
        }
    }

    private void readReply(ByteArrayOutputStream raw) throws IOException {
        String line = readLine(raw);
        char type = line.charAt(0);
        switch (type) {
            case '+', '-', ':' -> { }
            case '$' -> {
                int length = Integer.parseInt(line.substring(1));
                if (length >= 0) raw.write(in.readNBytes(length + 2));
            }
            case '*' -> {
                int count = Integer.parseInt(line.substring(1));
                for (int i = 0; i < count; i++) readReply(raw);
            }
            default -> throw new IOException("not a RESP reply: " + line);
        }
    }

    private String readLine(ByteArrayOutputStream raw) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int previous = -1;
        while (true) {
            int b = in.read();
            if (b == -1) throw new IOException("connection closed mid-reply");
            raw.write(b);
            if (previous == '\r' && b == '\n') break;
            if (previous != -1) line.write(previous);
            previous = b;
        }
        return line.toString(StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
