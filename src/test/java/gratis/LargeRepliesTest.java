package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Replies bigger than the socket's send buffer must arrive whole, however slowly the client reads them. */
class LargeRepliesTest extends ServerTest {
    private static final int ITEMS = 50_000;
    private static final String VALUE = "v".repeat(100);

    private static void fillList(RespClient a) throws Exception {
        // 400 values per RPUSH keeps each request under v1's 64 KB read buffer.
        for (int chunk = 0; chunk < ITEMS / 400; chunk++) {
            List<String> args = new ArrayList<>(List.of("RPUSH", "big"));
            for (int i = 0; i < 400; i++) args.add(VALUE);
            a.call(args.toArray(String[]::new));
        }
    }

    private static int arrayLength(String reply) {
        return Integer.parseInt(reply.substring(1, reply.indexOf("\r\n")));
    }

    @Test
    void aFiveMegabyteReplyArrivesWhole() throws Exception {
        RespClient a = client();
        fillList(a);
        String reply = a.call("LRANGE", "big", "0", "-1");
        assertEquals(ITEMS, arrayLength(reply));
        assertEquals(ITEMS * bulk(VALUE).length() + ("*" + ITEMS + "\r\n").length(), reply.length());
        assertEquals("+PONG\r\n", a.call("PING"));
    }

    @Test
    void aSlowReaderStillGetsItsWholeReplyAndOtherClientsAreServedMeanwhile() throws Exception {
        RespClient slow = client();
        fillList(slow);
        slow.send(RespClient.encode("LRANGE", "big", "0", "-1"));
        Thread.sleep(300);
        assertEquals("+PONG\r\n", client().call("PING"));
        assertEquals(ITEMS, arrayLength(slow.reply()));
        assertEquals(integer(ITEMS), slow.call("LLEN", "big"));
    }

    @Test
    void pipelinedRepliesKeepTheirOrderAcrossAPartialWrite() throws Exception {
        RespClient a = client();
        fillList(a);
        byte[] lrange = RespClient.encode("LRANGE", "big", "0", "-1");
        byte[] ping = RespClient.encode("ECHO", "after");
        byte[] both = new byte[lrange.length + ping.length];
        System.arraycopy(lrange, 0, both, 0, lrange.length);
        System.arraycopy(ping, 0, both, lrange.length, ping.length);
        a.send(both);
        assertEquals(ITEMS, arrayLength(a.reply()));
        assertEquals(bulk("after"), a.reply());
    }
}
