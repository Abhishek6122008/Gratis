package gratis;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Reading RESP requests: arrays of bulk strings, and inline commands (PING typed into nc). */
public final class Resp {
    /** Redis's limit for one bulk string. */
    public static final int MAX_BULK = 512 * 1024 * 1024;

    private Resp() {
    }

    /** A malformed request (a bad length line, say). Like Redis, the server closes that client. */
    public static final class ProtocolException extends IOException {
        public ProtocolException(String message) {
            super(message);
        }
    }

    /**
     * Parses one complete command from the buffer into args, as slices of the buffer's array, and moves the position
     * past it. Returns false, with the position unchanged, when the buffer doesn't hold a whole command yet. A line
     * that doesn't start with '*' is an inline command, split on spaces and tabs. The buffer must be array-backed.
     */
    public static boolean parse(ByteBuffer buffer, Args args) throws ProtocolException {
        byte[] a = buffer.array();
        int base = buffer.arrayOffset();
        int at = base + buffer.position();
        int limit = base + buffer.limit();
        args.clear();
        int end = lineEnd(a, at, limit);
        if (end < 0) return false;
        if (a[at] != '*') {
            for (int i = at; i < end; ) {
                while (i < end && (a[i] == ' ' || a[i] == '\t')) i++;
                int word = i;
                while (i < end && a[i] != ' ' && a[i] != '\t') i++;
                if (i > word) args.add(a, word, i - word);
            }
            buffer.position(end + 2 - base);
            return true;
        }
        long count = number(a, at + 1, end);
        at = end + 2;
        for (long n = 0; n < count; n++) {
            end = lineEnd(a, at, limit);
            if (end < 0) return false;
            if (a[at] != '$') throw new ProtocolException("expected '$', got '" + (char) a[at] + "'");
            long size = number(a, at + 1, end);
            if (size < 0 || size > MAX_BULK) throw new ProtocolException("invalid bulk length");
            at = end + 2;
            if (limit - at < size + 2) return false;
            args.add(a, at, (int) size);
            at += (int) size + 2;
        }
        buffer.position(at - base);
        return true;
    }

    /** The index of the '\r' of the first "\r\n" at or after from, or -1 if there is none before limit. */
    static int lineEnd(byte[] a, int from, int limit) {
        for (int i = from; i + 1 < limit; i++) {
            if (a[i] == '\r' && a[i + 1] == '\n') return i;
        }
        return -1;
    }

    /** A decimal number in a[from, to), read without making a String. */
    static long number(byte[] a, int from, int to) throws ProtocolException {
        boolean negative = from < to && a[from] == '-';
        int i = negative ? from + 1 : from;
        if (i == to || to - i > 18) throw new ProtocolException("invalid length");
        long value = 0;
        for (; i < to; i++) {
            if (a[i] < '0' || a[i] > '9') throw new ProtocolException("invalid length");
            value = value * 10 + (a[i] - '0');
        }
        return negative ? -value : value;
    }
}
