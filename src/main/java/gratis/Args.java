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
