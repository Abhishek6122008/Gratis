package gratis;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * A real Gratis server in its own JVM, on a free port. The tests only talk to it over a socket, the way a client
 * would, and every test gets a fresh one, so no state carries over from one test to the next.
 */
final class GratisServer implements AutoCloseable {
    private final Process process;
    final int port;

    private GratisServer(Process process, int port) {
        this.process = process;
        this.port = port;
    }

    static GratisServer start() throws IOException, InterruptedException {
        int port = freePort();
        String java = ProcessHandle.current().info().command().orElse("java");
        Process process = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), "Main", "--port",
                String.valueOf(port))
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
        GratisServer server = new GratisServer(process, port);
        server.awaitReady();
        return server;
    }

    RespClient client() throws IOException {
        return new RespClient(port);
    }

    List<RespClient> clients(int count) throws IOException {
        List<RespClient> clients = new ArrayList<>();
        for (int i = 0; i < count; i++) clients.add(client());
        return clients;
    }

    private void awaitReady() throws InterruptedException {
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) throw new IllegalStateException("server exited with " + process.exitValue());
            try (RespClient client = new RespClient(port)) {
                if (client.call("PING").equals("+PONG\r\n")) return;
            } catch (IOException notYet) {
                Thread.sleep(50);
            }
        }
        close();
        throw new IllegalStateException("server did not answer PING on port " + port + " within 10 s");
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Override
    public void close() {
        process.destroy();
        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
