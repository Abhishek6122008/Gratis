package gratis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** A growable reply buffer. Replies are added at the end; a socket write (or a copy) takes them from the front. */
final class Out {
    static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);

    ByteBuffer buf;

    Out(int capacity) {
        buf = ByteBuffer.allocate(capacity);
    }

    /** The buffer, with room for at least needed more bytes. It doubles when full and never shrinks. */
    ByteBuffer room(int needed) {
        if (buf.remaining() < needed) {
            int size = buf.capacity();
            while (size - buf.position() < needed) size *= 2;
            buf = ByteBuffer.allocate(size).put(buf.flip());
        }
        return buf;
    }

    int position() {
        return buf.position();
    }

    void position(int position) {
        buf.position(position);
    }

    void put(byte[] bytes) {
        room(bytes.length).put(bytes);
    }

    void put(String reply) {
        put(reply.getBytes(StandardCharsets.UTF_8));
    }

    void put(byte[] a, int offset, int length) {
        room(length).put(a, offset, length);
    }

    void putBulk(byte[] a, int offset, int length) {
        ByteBuffer out = room(length + 24);
        out.put((byte) '$');
        putNumber(out, length);
        out.put(CRLF).put(a, offset, length).put(CRLF);
    }

    void putInteger(long value) {
        ByteBuffer out = room(24);
        out.put((byte) ':');
        putNumber(out, value);
        out.put(CRLF);
    }

    /** Writes a decimal number as ASCII digits without making a String. */
    static void putNumber(ByteBuffer out, long value) {
        if (value == Long.MIN_VALUE) {
            out.put("-9223372036854775808".getBytes(StandardCharsets.US_ASCII));
            return;
        }
        if (value < 0) {
            out.put((byte) '-');
            value = -value;
        }
        int digits = 1;
        for (long rest = value; rest >= 10; rest /= 10) digits++;
        int end = out.position() + digits;
        for (int i = end - 1; i >= out.position(); i--) {
            out.put(i, (byte) ('0' + value % 10));
            value /= 10;
        }
        out.position(end);
    }
}
