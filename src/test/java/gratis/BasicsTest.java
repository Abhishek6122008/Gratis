package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class BasicsTest extends ServerTest {

    @Test
    void pingAndEcho() throws Exception {
        RespClient a = client();
        assertEquals("+PONG\r\n", a.call("PING"));
        assertEquals(bulk("hey"), a.call("ECHO", "hey"));
    }

    @Test
    void inlineCommandIsAccepted() throws Exception {
        RespClient a = client();
        a.send("PING\r\n".getBytes(StandardCharsets.UTF_8));
        assertEquals("+PONG\r\n", a.reply());
    }

    @Test
    void setThenGet() throws Exception {
        RespClient a = client();
        assertEquals(NIL, a.call("GET", "k"));
        assertEquals(OK, a.call("SET", "k", "v"));
        assertEquals(bulk("v"), a.call("GET", "k"));
        assertEquals(OK, a.call("SET", "k", "w"));
        assertEquals(bulk("w"), a.call("GET", "k"));
    }

    @Test
    void keyWithPxExpiresAfterItsTime() throws Exception {
        RespClient a = client();
        assertEquals(OK, a.call("SET", "k", "v", "PX", "100"));
        assertEquals(bulk("v"), a.call("GET", "k"));
        Thread.sleep(200);
        assertEquals(NIL, a.call("GET", "k"));
    }

    @Test
    void setWithoutExpiryClearsAnEarlierOne() throws Exception {
        RespClient a = client();
        assertEquals(OK, a.call("SET", "k", "v", "PX", "100"));
        assertEquals(OK, a.call("SET", "k", "w"));
        Thread.sleep(200);
        assertEquals(bulk("w"), a.call("GET", "k"));
    }

    @Test
    void pipelinedCommandsAreAnsweredInOrder() throws Exception {
        RespClient a = client();
        byte[] first = RespClient.encode("SET", "a", "1");
        byte[] second = RespClient.encode("GET", "a");
        byte[] third = RespClient.encode("INCR", "a");
        byte[] fourth = RespClient.encode("PING");
        byte[] all = new byte[first.length + second.length + third.length + fourth.length];
        int at = 0;
        for (byte[] part : new byte[][] {first, second, third, fourth}) {
            System.arraycopy(part, 0, all, at, part.length);
            at += part.length;
        }
        a.send(all);
        assertEquals(OK, a.reply());
        assertEquals(bulk("1"), a.reply());
        assertEquals(integer(2), a.reply());
        assertEquals("+PONG\r\n", a.reply());
    }

    @Test
    void incrCountsFromZeroAndKeepsGoing() throws Exception {
        RespClient a = client();
        assertEquals(integer(1), a.call("INCR", "fresh"));
        assertEquals(OK, a.call("SET", "n", "41"));
        assertEquals(integer(42), a.call("INCR", "n"));
        assertEquals(bulk("42"), a.call("GET", "n"));
    }

    @Test
    void incrRejectsNonIntegersAndOverflow() throws Exception {
        RespClient a = client();
        assertEquals(OK, a.call("SET", "foo", "xyz"));
        assertEquals(NOT_AN_INTEGER, a.call("INCR", "foo"));
        assertEquals(OK, a.call("SET", "big", String.valueOf(Long.MAX_VALUE)));
        assertEquals(NOT_AN_INTEGER, a.call("INCR", "big"));
        assertEquals(bulk(String.valueOf(Long.MAX_VALUE)), a.call("GET", "big"));
    }

    @Test
    void typeNamesEachKindOfKey() throws Exception {
        RespClient a = client();
        a.call("SET", "s", "v");
        a.call("RPUSH", "l", "x");
        a.call("XADD", "x", "1-1", "f", "v");
        assertEquals("+string\r\n", a.call("TYPE", "s"));
        assertEquals("+list\r\n", a.call("TYPE", "l"));
        assertEquals("+stream\r\n", a.call("TYPE", "x"));
        assertEquals("+none\r\n", a.call("TYPE", "missing"));
    }

    @Test
    void unknownCommandIsAnError() throws Exception {
        assertEquals("-ERR unknown command 'NOPE'\r\n", client().call("NOPE"));
    }
}
