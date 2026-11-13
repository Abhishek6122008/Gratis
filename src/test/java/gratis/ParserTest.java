package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Resp.parse on its own. The socket tests can't control where
 * TCP splits a request, so this one does: every split point of a pipelined stream.
 */
class ParserTest {
    private static final String STREAM = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$6\r\na\r\nb\r\n\r\n"
            + "PING\r\n"
            + "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"
            + "*1\r\n$0\r\n\r\n"
            + "  ECHO   hi  \r\n";
    private static final List<List<String>> COMMANDS = List.of(
            List.of("SET", "key", "a\r\nb\r\n"),
            List.of("PING"),
            List.of("GET", "key"),
            List.of(""),
            List.of("ECHO", "hi"));

    /** Feeds the stream in two reads split at {@code split}, parsing as the server does after each read. */
    private static List<List<String>> parseInTwo(byte[] bytes, int split) throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
        Args args = new Args();
        List<List<String>> parsed = new ArrayList<>();
        for (int[] part : new int[][] {{0, split}, {split, bytes.length}}) {
            buffer.put(bytes, part[0], part[1] - part[0]);
            buffer.flip();
            while (Resp.parse(buffer, args)) parsed.add(args.strings());
            buffer.compact();
        }
        assertEquals(0, buffer.position(), "bytes left over after the last command");
        return parsed;
    }

    @Test
    void everySplitPointGivesTheSameCommands() throws Exception {
        byte[] bytes = STREAM.getBytes(StandardCharsets.UTF_8);
        for (int split = 0; split <= bytes.length; split++) {
            assertEquals(COMMANDS, parseInTwo(bytes, split), "split at byte " + split);
        }
    }

    @Test
    void anIncompleteCommandLeavesThePositionWhereItWas() throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap("*2\r\n$3\r\nGET\r\n$3\r\nke".getBytes(StandardCharsets.UTF_8));
        assertFalse(Resp.parse(buffer, new Args()));
        assertEquals(0, buffer.position());
    }

    @Test
    void aMalformedLengthIsAProtocolError() {
        for (String bad : List.of("*x\r\n", "*1\r\n$abc\r\n", "*1\r\n:3\r\nabc\r\n", "*1\r\n$-5\r\n")) {
            ByteBuffer buffer = ByteBuffer.wrap(bad.getBytes(StandardCharsets.UTF_8));
            assertThrows(Resp.ProtocolException.class, () -> Resp.parse(buffer, new Args()), bad);
        }
    }
}
