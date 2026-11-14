package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ListsTest extends ServerTest {

    @Test
    void pushesReturnTheNewLength() throws Exception {
        RespClient a = client();
        assertEquals(integer(2), a.call("RPUSH", "l", "b", "c"));
        assertEquals(integer(3), a.call("LPUSH", "l", "a"));
        assertEquals(integer(3), a.call("LLEN", "l"));
        assertEquals(integer(0), a.call("LLEN", "missing"));
    }

    @Test
    void lpushAddsEachValueToTheHead() throws Exception {
        RespClient a = client();
        a.call("LPUSH", "l", "a", "b", "c");
        assertEquals(array(bulk("c"), bulk("b"), bulk("a")), a.call("LRANGE", "l", "0", "-1"));
    }

    @Test
    void lrangeTakesPositiveAndNegativeIndexes() throws Exception {
        RespClient a = client();
        a.call("RPUSH", "l", "a", "b", "c", "d", "e");
        assertEquals(array(bulk("b"), bulk("c")), a.call("LRANGE", "l", "1", "2"));
        assertEquals(array(bulk("d"), bulk("e")), a.call("LRANGE", "l", "-2", "-1"));
        assertEquals(array(bulk("a"), bulk("b"), bulk("c"), bulk("d"), bulk("e")), a.call("LRANGE", "l", "-9", "99"));
        assertEquals(array(), a.call("LRANGE", "l", "3", "1"));
        assertEquals(array(), a.call("LRANGE", "missing", "0", "-1"));
    }

    @Test
    void lpopTakesOneOrMany() throws Exception {
        RespClient a = client();
        a.call("RPUSH", "l", "a", "b", "c", "d");
        assertEquals(bulk("a"), a.call("LPOP", "l"));
        assertEquals(array(bulk("b"), bulk("c")), a.call("LPOP", "l", "2"));
        assertEquals(array(bulk("d")), a.call("LPOP", "l", "5"));
        assertEquals(NIL, a.call("LPOP", "l"));
        assertEquals(NIL_ARRAY, a.call("LPOP", "l", "2"));
    }

    @Test
    void blpopReturnsAtOnceWhenTheListHasItems() throws Exception {
        RespClient a = client();
        a.call("RPUSH", "l", "x", "y");
        assertEquals(array(bulk("l"), bulk("x")), a.call("BLPOP", "l", "0"));
        assertEquals(integer(1), a.call("LLEN", "l"));
    }

    @Test
    void blpopWaitsForALaterPush() throws Exception {
        RespClient waiter = client();
        RespClient pusher = client();
        waiter.send(RespClient.encode("BLPOP", "l", "0"));
        assertTrue(waiter.silentFor(Duration.ofMillis(200)), "BLPOP answered before anything was pushed");
        assertEquals(integer(1), pusher.call("RPUSH", "l", "x"));
        assertEquals(array(bulk("l"), bulk("x")), waiter.reply());
        assertEquals(integer(0), pusher.call("LLEN", "l"));
    }

    @Test
    void blpopWaitersAreServedInArrivalOrder() throws Exception {
        RespClient first = client();
        RespClient second = client();
        RespClient pusher = client();
        first.send(RespClient.encode("BLPOP", "l", "0"));
        assertTrue(first.silentFor(Duration.ofMillis(100)));
        second.send(RespClient.encode("BLPOP", "l", "0"));
        assertTrue(second.silentFor(Duration.ofMillis(100)));
        pusher.call("RPUSH", "l", "x");
        assertEquals(array(bulk("l"), bulk("x")), first.reply());
        assertTrue(second.silentFor(Duration.ofMillis(200)), "the second waiter got an item that went to the first");
    }

    @Test
    void blpopTimesOutWithNil() throws Exception {
        assertEquals(NIL_ARRAY, client().call("BLPOP", "l", "0.1"));
    }
}
