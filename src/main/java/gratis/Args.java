package gratis;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * One command's arguments, as slices of a byte array: the read buffer while the command is parsed and run. Each
 * connection reuses one Args, so parsing a command allocates nothing. A command that must outlive the read buffer (one
 * queued by MULTI) is copied.
 */
public final class Args {
    private byte[][] arrays = new byte[8][];
    private int[] offsets = new int[8];
    private int[] lengths = new int[8];
    private int count;
    // The view key(i) returns: one per Args, so one per connection, and each is used by one thread at a time.
    private final Key view = new Key();

    public void clear() {
        count = 0;
    }

    public void add(byte[] array, int offset, int length) {
        if (count == arrays.length) {
            arrays = Arrays.copyOf(arrays, count * 2);
            offsets = Arrays.copyOf(offsets, count * 2);
            lengths = Arrays.copyOf(lengths, count * 2);
        }
        arrays[count] = array;
        offsets[count] = offset;
        lengths[count] = length;
        count++;
    }

    public int size() {
        return count;
    }

    public byte[] array(int i) {
        check(i);
        return arrays[i];
    }

    public int offset(int i) {
        check(i);
        return offsets[i];
    }

    public int length(int i) {
        check(i);
        return lengths[i];
    }

    public String string(int i) {
        check(i);
        return new String(arrays[i], offsets[i], lengths[i], StandardCharsets.UTF_8);
    }

    /** Argument i as a key to look up: a reusable view, valid until the next call. Store {@link Key#copy()}. */
    public Key key(int i) {
        check(i);
        return view.set(arrays[i], offsets[i], lengths[i]);
    }

    /** Argument i as its own array. */
    public byte[] bytes(int i) {
        check(i);
        return Arrays.copyOfRange(arrays[i], offsets[i], offsets[i] + lengths[i]);
    }

    /** Copies argument i into target, which must be exactly length(i) long. */
    public void copyInto(int i, byte[] target) {
        check(i);
        System.arraycopy(arrays[i], offsets[i], target, 0, lengths[i]);
    }

    /** Argument i as a decimal number, read without making a String. Like Long.parseLong, a bad one throws. */
    public long number(int i) {
        check(i);
        byte[] a = arrays[i];
        int at = offsets[i];
        int end = at + lengths[i];
        boolean negative = at < end && a[at] == '-';
        if (negative) at++;
        if (at == end) throw new NumberFormatException("empty number");
        long value = 0;
        for (; at < end; at++) {
            int digit = a[at] - '0';
            if (digit < 0 || digit > 9 || value > (Long.MAX_VALUE - digit) / 10) {
                throw new NumberFormatException("not a number");
            }
            value = value * 10 + digit;
        }
        return negative ? -value : value;
    }

    /** Whether argument i is the given upper-case ASCII word, in any case, without making a String. */
    public boolean is(int i, String upper) {
        if (i >= count || lengths[i] != upper.length()) return false;
        byte[] a = arrays[i];
        int at = offsets[i];
        for (int j = 0; j < lengths[i]; j++) {
            int b = a[at + j];
            if (b >= 'a' && b <= 'z') b -= 32;
            if (b != upper.charAt(j)) return false;
        }
        return true;
    }

    /** A copy that owns its bytes, for a command that must outlive the read buffer (one queued by MULTI). */
    public Args copy() {
        Args copy = new Args();
        for (int i = 0; i < count; i++) copy.add(bytes(i), 0, lengths[i]);
        return copy;
    }

    public List<String> strings() {
        List<String> strings = new ArrayList<>(count);
        for (int i = 0; i < count; i++) strings.add(string(i));
        return strings;
    }

    /** Throws IndexOutOfBoundsException, which the server answers with "wrong number of arguments". */
    public void check(int i) {
        if (i >= count) throw new IndexOutOfBoundsException("argument " + i + " of " + count);
    }
}
