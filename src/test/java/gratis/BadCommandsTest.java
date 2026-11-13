package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
    void insideExecABadCommandIsAnErrorEntry() throws Exception {
        RespClient a = client();
        a.call("MULTI");
        a.call("GET");
        a.call("SET", "k", "v");
        assertEquals(array("-ERR wrong number of arguments for 'get' command\r\n", OK), a.call("EXEC"));
        assertTrue(a.call("GET", "k").endsWith("v\r\n"));
    }
}
