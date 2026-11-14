package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** A malformed command gets an error reply, and the server keeps serving every client. */
class BadCommandsTest extends ServerTest {

    @Test
    void missingArgumentsAreAnError() throws Exception {
        RespClient a = client();
        assertEquals("-ERR wrong number of arguments for 'get' command\r\n", a.call("GET"));
        assertEquals("-ERR wrong number of arguments for 'lrange' command\r\n", a.call("LRANGE", "l"));
        assertEquals("+PONG\r\n", a.call("PING"));
        assertEquals("+PONG\r\n", client().call("PING"));
    }

    @Test
    void aNumberThatIsNotANumberIsAnError() throws Exception {
        RespClient a = client();
        assertEquals(NOT_AN_INTEGER, a.call("SET", "k", "v", "EX", "soon"));
        assertEquals(NOT_AN_INTEGER, a.call("LRANGE", "l", "first", "last"));
        assertEquals(NOT_AN_INTEGER, a.call("BLPOP", "l", "never"));
        assertEquals("+PONG\r\n", a.call("PING"));
    }

    @Test
    void aMalformedRequestClosesOnlyThatClient() throws Exception {
        RespClient other = client();
        other.call("SET", "k", "v");
        try (Socket bad = new Socket("127.0.0.1", port())) {
            bad.setSoTimeout(5_000);
            bad.getOutputStream().write("*1\r\n$abc\r\n".getBytes(StandardCharsets.UTF_8));
            assertEquals(-1, bad.getInputStream().read(), "the server should close a client that breaks the protocol");
        }
        assertEquals(bulk("v"), other.call("GET", "k"));
        assertEquals("+PONG\r\n", client().call("PING"));
    }

    @Test
    void insideExecABadCommandIsAnErrorEntry() throws Exception {
        RespClient a = client();
        a.call("MULTI");
        a.call("GET");
        a.call("SET", "k", "v");
        assertEquals(array("-ERR wrong number of arguments for 'get' command\r\n", OK), a.call("EXEC"));
        assertTrue(a.call("GET", "k").endsWith("v\r\n"));
    }
}
