package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class StreamsTest extends ServerTest {
    private static final String NOT_GREATER_THAN_TOP =
            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";

    private static String entry(String id, String... fields) {
        String[] encoded = new String[fields.length];
        for (int i = 0; i < fields.length; i++) encoded[i] = bulk(fields[i]);
        return array(bulk(id), array(encoded));
    }

    @Test
    void xaddChecksThatIdsGrow() throws Exception {
        RespClient a = client();
        assertEquals("-ERR The ID specified in XADD must be greater than 0-0\r\n", a.call("XADD", "s", "0-0", "f", "v"));
        assertEquals(bulk("1-1"), a.call("XADD", "s", "1-1", "f", "v"));
        assertEquals(NOT_GREATER_THAN_TOP, a.call("XADD", "s", "1-1", "f", "v"));
        assertEquals(NOT_GREATER_THAN_TOP, a.call("XADD", "s", "0-5", "f", "v"));
        assertEquals(bulk("1-2"), a.call("XADD", "s", "1-2", "f", "v"));
    }

    @Test
    void xaddFillsInTheSequence() throws Exception {
        RespClient a = client();
        assertEquals(bulk("0-1"), a.call("XADD", "s", "0-*", "f", "v"));
        assertEquals(bulk("5-0"), a.call("XADD", "s", "5-*", "f", "v"));
        assertEquals(bulk("5-1"), a.call("XADD", "s", "5-*", "f", "v"));
    }

    @Test
    void xaddWithAStarIdUsesTheClock() throws Exception {
        long before = System.currentTimeMillis();
        String reply = client().call("XADD", "s", "*", "f", "v");
        long ms = Long.parseLong(reply.split("\r\n")[1].split("-")[0]);
        assertTrue(ms >= before && ms <= System.currentTimeMillis(), reply);
    }

    @Test
    void xrangeReturnsEntriesBetweenTwoIds() throws Exception {
        RespClient a = client();
        a.call("XADD", "s", "1-1", "a", "1");
        a.call("XADD", "s", "2-1", "b", "2");
        a.call("XADD", "s", "3-1", "c", "3");
        assertEquals(array(entry("2-1", "b", "2"), entry("3-1", "c", "3")), a.call("XRANGE", "s", "2", "+"));
        assertEquals(array(entry("1-1", "a", "1")), a.call("XRANGE", "s", "-", "1-1"));
    }

    @Test
    void xreadReturnsEntriesAfterAnId() throws Exception {
        RespClient a = client();
        a.call("XADD", "s", "1-1", "a", "1");
        a.call("XADD", "s", "2-1", "b", "2");
        assertEquals(array(array(bulk("s"), array(entry("2-1", "b", "2")))), a.call("XREAD", "STREAMS", "s", "1-1"));
    }

    @Test
    void xreadBlockWaitsForTheNextEntry() throws Exception {
        RespClient reader = client();
        RespClient writer = client();
        writer.call("XADD", "s", "1-1", "old", "x");
        reader.send(RespClient.encode("XREAD", "BLOCK", "0", "STREAMS", "s", "$"));
        assertTrue(reader.silentFor(Duration.ofMillis(200)), "XREAD with $ returned an existing entry");
        writer.call("XADD", "s", "2-1", "new", "y");
        assertEquals(array(array(bulk("s"), array(entry("2-1", "new", "y")))), reader.reply());
    }

    @Test
    void xreadBlockTimesOutWithNil() throws Exception {
        assertEquals(NIL_ARRAY, client().call("XREAD", "BLOCK", "100", "STREAMS", "s", "0-0"));
    }
}
