package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * Replies come back in the order the commands were sent, even when one of them blocks: like Redis, a blocked client's
 * later commands wait until it is served or times out. Each test sends its commands in one write, so the server reads
 * them together.
 */
class PipeliningTest extends ServerTest {
    private static final String PONG = "+PONG\r\n";

    private static byte[] pipeline(String[]... commands) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String[] command : commands) out.writeBytes(RespClient.encode(command));
        return out.toByteArray();
    }

    @Test
    void manyCommandsInOneWriteGetTheirRepliesInOrder() throws Exception {
        RespClient a = client();
        a.send(pipeline(new String[] {"SET", "k", "1"}, new String[] {"INCR", "k"}, new String[] {"GET", "k"},
                new String[] {"PING"}));
        assertEquals(OK, a.reply());
        assertEquals(integer(2), a.reply());
        assertEquals(bulk("2"), a.reply());
        assertEquals(PONG, a.reply());
    }

    @Test
    void commandsAfterABlpopWaitUntilItIsServed() throws Exception {
        RespClient waiter = client();
        RespClient pusher = client();
        waiter.send(pipeline(new String[] {"BLPOP", "l", "0"}, new String[] {"PING"}));
        assertTrue(waiter.silentFor(Duration.ofMillis(200)), "PING was answered while BLPOP was still waiting");
        pusher.call("RPUSH", "l", "x");
        assertEquals(array(bulk("l"), bulk("x")), waiter.reply());
        assertEquals(PONG, waiter.reply());
    }

    @Test
    void commandsAfterABlpopRunWhenItTimesOut() throws Exception {
        RespClient waiter = client();
        waiter.send(pipeline(new String[] {"BLPOP", "l", "0.1"}, new String[] {"PING"}));
        assertEquals(NIL_ARRAY, waiter.reply());
        assertEquals(PONG, waiter.reply());
    }

    @Test
    void commandsAfterAnXreadBlockWaitForTheEntry() throws Exception {
        RespClient reader = client();
        RespClient writer = client();
        reader.send(pipeline(new String[] {"XREAD", "BLOCK", "0", "STREAMS", "s", "$"}, new String[] {"PING"}));
        assertTrue(reader.silentFor(Duration.ofMillis(200)), "PING was answered while XREAD was still waiting");
        writer.call("XADD", "s", "1-1", "f", "v");
        assertEquals(array(array(bulk("s"), array(array(bulk("1-1"), array(bulk("f"), bulk("v")))))),
                reader.reply());
        assertEquals(PONG, reader.reply());
    }

    @Test
    void aServedClientsWaitingPushCanServeTheNextWaiter() throws Exception {
        RespClient first = client();
        RespClient second = client();
        RespClient pusher = client();
        second.send(RespClient.encode("BLPOP", "b", "0"));
        assertTrue(second.silentFor(Duration.ofMillis(100)));
        first.send(pipeline(new String[] {"BLPOP", "a", "0"}, new String[] {"RPUSH", "b", "y"}));
        assertTrue(first.silentFor(Duration.ofMillis(100)), "RPUSH ran while BLPOP was still waiting");
        pusher.call("RPUSH", "a", "x");
        assertEquals(array(bulk("a"), bulk("x")), first.reply());
        assertEquals(integer(1), first.reply());
        assertEquals(array(bulk("b"), bulk("y")), second.reply());
    }
}
