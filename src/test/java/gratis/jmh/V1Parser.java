package gratis.jmh;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** v1's request parser, copied verbatim from v1-baseline (Main.parse and Main.line), as the JMH baseline. */
final class V1Parser {
    private V1Parser() {
    }

    static List<String> parse(ByteBuffer buffer) {
        int start = buffer.position();
        String header = line(buffer);
        if (header != null && !header.startsWith("*")) {
            return List.of(header.trim().split("\\s+"));
        }
        if (header != null) {
            int count = Integer.parseInt(header.substring(1));
            List<String> args = new ArrayList<>();
            while (args.size() < count) {
                String length = line(buffer);
                if (length == null) break;
                int size = Integer.parseInt(length.substring(1));
                if (buffer.remaining() < size + 2) break;
                byte[] bytes = new byte[size];
                buffer.get(bytes);
                buffer.position(buffer.position() + 2);
                args.add(new String(bytes, StandardCharsets.UTF_8));
            }
            if (args.size() == count) return args;
        }
        buffer.position(start);
        return null;
    }

    static String line(ByteBuffer buffer) {
        for (int i = buffer.position(); i + 1 < buffer.limit(); i++) {
            if (buffer.get(i) == '\r' && buffer.get(i + 1) == '\n') {
                byte[] bytes = new byte[i - buffer.position()];
                buffer.get(bytes);
                buffer.position(i + 2);
                return new String(bytes, StandardCharsets.UTF_8);
            }
        }
        return null;
    }
}
