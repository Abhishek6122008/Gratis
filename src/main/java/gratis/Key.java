package gratis;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A string key as bytes. Stored keys own their array. {@link #LOOKUP} is one reusable view into a command's arguments:
 * HashMap only calls hashCode and equals on the key passed to get, replace and remove, and never keeps it, so looking a
 * key up allocates nothing. Anything that stores a key stores {@link #copy()}. One thread only, like the server.
 */
public final class Key {
    static final Key LOOKUP = new Key();

    private byte[] bytes;
    private int offset;
    private int length;
    private int hash;

    public static Key of(String key) {
        byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
        return new Key().set(bytes, 0, bytes.length);
    }

    Key set(byte[] bytes, int offset, int length) {
        this.bytes = bytes;
        this.offset = offset;
        this.length = length;
        int h = 1;
        for (int i = offset; i < offset + length; i++) h = 31 * h + bytes[i];
        hash = h;
        return this;
    }

    /** A key that owns a copy of these bytes, safe to store. */
    public Key copy() {
        return new Key().set(Arrays.copyOfRange(bytes, offset, offset + length), 0, length);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Key k && k.length == length
                && Arrays.equals(bytes, offset, offset + length, k.bytes, k.offset, k.offset + k.length);
    }
}
