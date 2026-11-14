package gratis;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/** String values are bytes: what goes in comes back unchanged, valid UTF-8 or not. */
class BinaryValuesTest extends ServerTest {
    private static final byte[] NOT_UTF8 = {(byte) 0xff, (byte) 0xfe, 0x00, 0x41};

    private static byte[] bulkOf(byte[] value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(("$" + value.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.writeBytes(value);
        out.writeBytes("\r\n".getBytes(StandardCharsets.US_ASCII));
        return out.toByteArray();
    }

    private static byte[] ascii(String s) {
        return s.getBytes(StandardCharsets.US_ASCII);
    }

    @Test
    void aValueThatIsNotUtf8RoundTripsByteForByte() throws Exception {
        RespClient a = client();
        a.send(RespClient.encode(ascii("SET"), ascii("k"), NOT_UTF8));
        assertEquals(OK, a.reply());
        a.send(RespClient.encode(ascii("GET"), ascii("k")));
        assertArrayEquals(bulkOf(NOT_UTF8), a.replyBytes());
    }

    @Test
    void aKeyThatIsNotUtf8IsItsOwnKey() throws Exception {
        RespClient a = client();
        byte[] other = {(byte) 0xff, (byte) 0xfd, 0x00, 0x41};
        a.send(RespClient.encode(ascii("SET"), NOT_UTF8, ascii("one")));
        assertEquals(OK, a.reply());
        a.send(RespClient.encode(ascii("SET"), other, ascii("two")));
        assertEquals(OK, a.reply());
        a.send(RespClient.encode(ascii("GET"), NOT_UTF8));
        assertEquals(bulk("one"), a.reply());
    }

    @Test
    void overwritingWithAValueOfTheSameLengthReplacesItWhole() throws Exception {
        RespClient a = client();
        a.call("SET", "k", "aaaa");
        a.send(RespClient.encode(ascii("SET"), ascii("k"), NOT_UTF8));
        assertEquals(OK, a.reply());
        a.send(RespClient.encode(ascii("GET"), ascii("k")));
        assertArrayEquals(bulkOf(NOT_UTF8), a.replyBytes());
        assertEquals(OK, a.call("SET", "k", "bb"));
        assertEquals(bulk("bb"), a.call("GET", "k"));
    }

    @Test
    void aBadSetOptionLeavesTheKeyAsItWas() throws Exception {
        RespClient a = client();
        a.call("SET", "k", "old");
        assertEquals(NOT_AN_INTEGER, a.call("SET", "k", "new", "EX", "soon"));
        assertEquals(bulk("old"), a.call("GET", "k"));
    }
}
