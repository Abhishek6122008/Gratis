package gratis;

/**
 * Which shard owns a key: Redis Cluster's hash slot (CRC16 of the key, mod 16384), then the slot mod the shard count.
 * Hash tags work as in Redis Cluster: if the key has a non-empty {...}, only the part inside the first such braces is
 * hashed, so "{user1}:name" and "{user1}:email" always live on the same shard.
 */
public final class Slots {
    public static final int COUNT = 16384;
    private static final int[] TABLE = new int[256];

    static {
        // CRC16-CCITT (XMODEM): polynomial 0x1021, initial value 0, no reflection. The one Redis Cluster uses.
        for (int i = 0; i < 256; i++) {
            int crc = i << 8;
            for (int bit = 0; bit < 8; bit++) crc = (crc & 0x8000) != 0 ? (crc << 1) ^ 0x1021 : crc << 1;
            TABLE[i] = crc & 0xFFFF;
        }
    }

    private Slots() {
    }

    public static int crc16(byte[] a, int from, int to) {
        int crc = 0;
        for (int i = from; i < to; i++) crc = ((crc << 8) ^ TABLE[((crc >>> 8) ^ a[i]) & 0xFF]) & 0xFFFF;
        return crc;
    }

    /** The key's hash slot, 0 to 16383. */
    public static int slot(byte[] a, int offset, int length) {
        int from = offset;
        int to = offset + length;
        for (int open = from; open < to; open++) {
            if (a[open] != '{') continue;
            for (int close = open + 1; close < to; close++) {
                if (a[close] != '}') continue;
                if (close > open + 1) {
                    from = open + 1;
                    to = close;
                }
                break;
            }
            break;
        }
        return crc16(a, from, to) & (COUNT - 1);
    }

    public static int slot(byte[] key) {
        return slot(key, 0, key.length);
    }
}
