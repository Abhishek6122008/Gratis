package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Requests bigger than the first 64 KB read buffer are answered like any other. */
class LargeRequestsTest extends ServerTest {

    @Test
    void aSetOfTwoHundredKilobytesIsStoredWhole() throws Exception {
        RespClient a = client();
        String value = "x".repeat(200 * 1024);
        assertEquals(OK, a.call("SET", "k", value));
        assertEquals(bulk(value), a.call("GET", "k"));
    }

    @Test
    void aFiveMegabyteValueRoundTrips() throws Exception {
        RespClient a = client();
        String value = "y".repeat(5 * 1024 * 1024);
        assertEquals(OK, a.call("SET", "k", value));
        assertEquals(bulk(value), a.call("GET", "k"));
        assertEquals("+PONG\r\n", a.call("PING"));
    }
}
