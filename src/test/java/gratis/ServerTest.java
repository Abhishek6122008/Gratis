package gratis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Gives each test a fresh server, and closes the clients the test opened. */
abstract class ServerTest {
    static final String OK = "+OK\r\n";
    static final String NIL = "$-1\r\n";
    static final String NIL_ARRAY = "*-1\r\n";
    static final String QUEUED = "+QUEUED\r\n";
    static final String NOT_AN_INTEGER = "-ERR value is not an integer or out of range\r\n";

    private GratisServer server;
    private final List<RespClient> clients = new ArrayList<>();

    @BeforeEach
    void startServer() throws Exception {
        server = GratisServer.start(shards());
    }

    /**
     * How many shards the test's server runs. Four, so every test also crosses between shards: clients are handed to
     * shards in turn, and keys spread over all four.
     */
    int shards() {
        return 4;
    }

    @AfterEach
    void stopServer() throws IOException {
        for (RespClient client : clients) client.close();
        server.close();
    }

    RespClient client() throws IOException {
        RespClient client = server.client();
        clients.add(client);
        return client;
    }

    int port() {
        return server.port;
    }

    static String bulk(String value) {
        return "$" + value.length() + "\r\n" + value + "\r\n";
    }

    static String integer(long value) {
        return ":" + value + "\r\n";
    }

    static String array(String... encodedElements) {
        return "*" + encodedElements.length + "\r\n" + String.join("", encodedElements);
    }
}
