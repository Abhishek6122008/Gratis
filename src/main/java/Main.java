import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gratis.Args;
import gratis.Key;
import gratis.Resp;

public class Main {
    // String keys and values are bytes, so any value round-trips unchanged (v1 decoded them as UTF-8 text).
    static final Map<Key, byte[]> store = new HashMap<>();
    static final Map<Key, Long> expiries = new HashMap<>();
    static final Map<String, List<String>> lists = new HashMap<>();
    static final Map<String, Deque<Waiter>> blocked = new HashMap<>();

    static final Map<String, List<Entry>> streams = new HashMap<>();
    static final List<Reader> readers = new ArrayList<>();
    static boolean executing;
    static Selector selector;
    // Clients with replies waiting in their output buffer, flushed once per loop pass.
    static final Set<SelectionKey> pending = new LinkedHashSet<>();
    // Clients unblocked in this loop pass, whose pipelined commands after the blocking one are still to run.
    static final Deque<SelectionKey> resumed = new ArrayDeque<>();

    // Each client's unparsed input, and the replies not yet written to its socket.
    static final class Connection {
        ByteBuffer in = ByteBuffer.allocate(64 * 1024);
        ByteBuffer out = ByteBuffer.allocate(16 * 1024);
        final Args args = new Args();
        // The commands queued after MULTI, or null outside a transaction.
        List<Args> queue;
        // Waiting in BLPOP or XREAD BLOCK: commands it sends meanwhile stay unparsed in `in` until it is answered.
        boolean blocked;
    }

    record Waiter(SocketChannel client, long deadline) {}

    record Reader(SocketChannel client, List<String> keys, List<long[]> after, long deadline) {}

    record Entry(long ms, long seq, List<String> fields) {
        String id() {
            return ms + "-" + seq;
        }
    }

    public static void main(String[] args) {
        int port = 6379;
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
        }
        System.out.println("Redis server started on port " + port);
        try {
            selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(port));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                selector.select(nextTimeout());
                Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                while (keys.hasNext()) {
                    SelectionKey key = keys.next();
                    keys.remove();
                    if (key.isAcceptable()) {
                        ServerSocketChannel server = (ServerSocketChannel) key.channel();
                        SocketChannel client = server.accept();
                        if (client == null) continue;
                        client.configureBlocking(false);
                        client.register(selector, SelectionKey.OP_READ, new Connection());
                        System.out.println("Client connected!");
                    }
                    if (key.isValid() && key.isWritable()) pending.add(key);
                    if (key.isValid() && key.isReadable()) {
                        try {
                            read(key);
                        } catch (IOException e) {
                            disconnect((SocketChannel) key.channel());
                        }
                    }
                }
                expire();
                // Clients answered in this pass run what they sent after the blocking command. That can answer others.
                while (!resumed.isEmpty()) {
                    SelectionKey key = resumed.poll();
                    if (!key.isValid()) continue;
                    try {
                        process(key);
                    } catch (IOException e) {
                        disconnect((SocketChannel) key.channel());
                    }
                }
                for (SelectionKey key : pending) flush(key);
                pending.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Redis's limit for one bulk string; a request still incomplete at this size ends the client.
    static final int MAX_REQUEST = 512 * 1024 * 1024;

    // One client's failed read must not stop the server for everyone else, so its IOException ends only that client.
    static void read(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        Connection connection = (Connection) key.attachment();
        ByteBuffer buffer = connection.in;
        if (!buffer.hasRemaining()) {
            // The buffer holds one incomplete command that fills it: make room for the rest.
            if (buffer.capacity() >= MAX_REQUEST) throw new IOException("request too large");
            buffer = connection.in = ByteBuffer.allocate(Math.min(MAX_REQUEST, buffer.capacity() * 2)).put(buffer.flip());
        }
        if (client.read(buffer) == -1) {
            disconnect(client);
            return;
        }
        process(key);
    }

    // Runs every complete command in the client's input buffer. A command that blocks stops the loop: like Redis, the
    // client's later commands wait until it is answered, so its replies stay in the order it sent the commands.
    static void process(SelectionKey key) throws IOException {
        SocketChannel client = (SocketChannel) key.channel();
        Connection connection = (Connection) key.attachment();
        ByteBuffer buffer = connection.in.flip();
        Args args = connection.args;
        while (!connection.blocked && Resp.parse(buffer, args)) {
            if (args.size() > 0) run(args, client, connection);
        }
        buffer.compact();
        if (connection.out.position() > 0) pending.add(key);
    }

    static void disconnect(SocketChannel client) {
        try {
            client.close();
        } catch (IOException ignored) {
            // closing anyway
        }
        System.out.println("Client disconnected!");
    }

    // Runs one command and puts its reply in the client's output buffer. A command that throws gets an error reply, the
    // way Redis answers bad arguments, instead of ending the loop; anything it wrote before throwing is dropped.
    static void run(Args args, SocketChannel client, Connection connection) throws IOException {
        int mark = connection.out.position();
        String reply;
        try {
            reply = handle(args, client, connection);
        } catch (IndexOutOfBoundsException e) {
            connection.out.position(mark);
            reply = "-ERR wrong number of arguments for '" + args.string(0).toLowerCase() + "' command\r\n";
        } catch (NumberFormatException e) {
            connection.out.position(mark);
            reply = NOT_AN_INTEGER;
        } catch (RuntimeException e) {
            connection.out.position(mark);
            reply = "-ERR " + e.getMessage() + "\r\n";
        }
        if (reply != null) write(connection, reply);
    }

    // The string commands are the hot path (memtier sends only SET and GET). They read their arguments as bytes, write
    // their reply straight into the output buffer and return null. Every other command goes through handleOther with
    // its arguments as Strings and returns its reply, or null when the client is now blocked.
    static String handle(Args args, SocketChannel client, Connection c) throws IOException {
        if (c.queue != null && !args.is(0, "MULTI") && !args.is(0, "EXEC") && !args.is(0, "DISCARD")) {
            c.queue.add(args.copy());
            return "+QUEUED\r\n";
        }
        if (args.is(0, "GET")) {
            byte[] value = get(args.key(1));
            if (value == null) writeBytes(c, NIL);
            else writeBulk(c, value, 0, value.length);
            return null;
        }
        if (args.is(0, "SET")) {
            // Options first, so a bad one leaves the key untouched (v1 stored the value, then failed on the option).
            long expiresAt = 0;
            for (int i = 3; i + 1 < args.size(); i += 2) {
                long amount = args.number(i + 1);
                if (args.is(i, "EX")) expiresAt = System.currentTimeMillis() + amount * 1000;
                else if (args.is(i, "PX")) expiresAt = System.currentTimeMillis() + amount;
            }
            Key key = args.key(1);
            int length = args.length(2);
            byte[] old = store.get(key);
            if (old != null && old.length == length) args.copyInto(2, old); // same size: reuse the array
            else if (old != null) store.replace(key, args.bytes(2));
            else store.put(key.copy(), args.bytes(2));
            if (!expiries.isEmpty()) expiries.remove(key);
            if (expiresAt != 0) expiries.put(key.copy(), expiresAt);
            writeBytes(c, OK);
            return null;
        }
        if (args.is(0, "INCR")) {
            Key key = args.key(1);
            byte[] current = get(key);
            long value;
            try {
                long now = current == null ? 0 : Long.parseLong(new String(current, StandardCharsets.US_ASCII));
                value = Math.addExact(now, 1);
            } catch (NumberFormatException | ArithmeticException e) {
                return NOT_AN_INTEGER;
            }
            byte[] digits = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
            if (current == null) store.put(key.copy(), digits);
            else store.replace(key, digits);
            writeInteger(c, value);
            return null;
        }
        if (args.is(0, "PING")) {
            writeBytes(c, PONG);
            return null;
        }
        if (args.is(0, "ECHO")) {
            args.check(1);
            writeBulk(c, args.array(1), args.offset(1), args.length(1));
            return null;
        }
        if (args.is(0, "MULTI")) {
            if (c.queue != null) return "-ERR MULTI calls can not be nested\r\n";
            c.queue = new ArrayList<>();
            return "+OK\r\n";
        }
        if (args.is(0, "EXEC")) {
            List<Args> queued = c.queue;
            if (queued == null) return "-ERR EXEC without MULTI\r\n";
            c.queue = null;
            write(c, "*" + queued.size() + "\r\n");
            executing = true;
            try {
                for (Args command : queued) run(command, client, c);
            } finally {
                executing = false;
            }
            return null;
        }
        if (args.is(0, "DISCARD")) {
            if (c.queue == null) return "-ERR DISCARD without MULTI\r\n";
            c.queue = null;
            return "+OK\r\n";
        }
        return handleOther(args.strings(), client, c);
    }

    static String handleOther(List<String> command, SocketChannel client, Connection c) throws IOException {
        String name = command.get(0).toUpperCase();
        return switch (name) {
            case "TYPE" -> "+" + type(command.get(1)) + "\r\n";
            case "XADD" -> {
                List<Entry> stream = streams.getOrDefault(command.get(1), List.of());
                Entry last = stream.isEmpty() ? null : stream.get(stream.size() - 1);
                String[] id = command.get(2).equals("*") ? new String[] {String.valueOf(System.currentTimeMillis()), "*"} : command.get(2).split("-");
                long ms = Long.parseLong(id[0]);
                long seq = id[1].equals("*") ? nextSeq(last, ms) : Long.parseLong(id[1]);
                Entry entry = new Entry(ms, seq, List.copyOf(command.subList(3, command.size())));
                if (ms == 0 && seq == 0) yield "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                if (last != null && (ms < last.ms() || (ms == last.ms() && seq <= last.seq()))) {
                    yield "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                }
                streams.computeIfAbsent(command.get(1), k -> new ArrayList<>()).add(entry);
                wake(command.get(1));
                yield bulk(entry.id());
            }
            case "XRANGE" -> {
                long[] start = parseId(command.get(2), 0);
                long[] end = parseId(command.get(3), Long.MAX_VALUE);
                List<Object> result = new ArrayList<>();
                for (Entry entry : streams.getOrDefault(command.get(1), List.of())) {
                    if (compare(entry, start) >= 0 && compare(entry, end) <= 0) result.add(List.of(entry.id(), entry.fields()));
                }
                yield array(result);
            }
            case "XREAD" -> {
                int s = 1;
                long block = -1;
                while (!command.get(s).equalsIgnoreCase("STREAMS")) {
                    if (command.get(s).equalsIgnoreCase("BLOCK")) block = Long.parseLong(command.get(s + 1));
                    s++;
                }
                int n = (command.size() - s - 1) / 2;
                List<String> keys = command.subList(s + 1, s + 1 + n);
                List<long[]> after = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    String id = command.get(s + 1 + n + i);
                    after.add(id.equals("$") ? lastId(keys.get(i)) : parseId(id, 0));
                }
                List<Object> result = xread(keys, after);
                if (!result.isEmpty()) yield array(result);
                if (block < 0 || executing) yield "*-1\r\n";
                long deadline = block == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + block;
                readers.add(new Reader(client, List.copyOf(keys), after, deadline));
                c.blocked = true;
                yield null;
            }
            case "RPUSH" -> {
                List<String> list = lists.computeIfAbsent(command.get(1), k -> new ArrayList<>());
                list.addAll(command.subList(2, command.size()));
                String reply = ":" + list.size() + "\r\n";
                serve(command.get(1));
                yield reply;
            }
            case "LPUSH" -> {
                List<String> list = lists.computeIfAbsent(command.get(1), k -> new ArrayList<>());
                for (String value : command.subList(2, command.size())) list.add(0, value);
                String reply = ":" + list.size() + "\r\n";
                serve(command.get(1));
                yield reply;
            }
            case "LLEN" -> ":" + lists.getOrDefault(command.get(1), List.of()).size() + "\r\n";
            case "LPOP" -> {
                List<String> list = lists.get(command.get(1));
                boolean multiple = command.size() > 2;
                if (list == null || list.isEmpty()) yield multiple ? "*-1\r\n" : "$-1\r\n";
                int count = multiple ? Math.min(Integer.parseInt(command.get(2)), list.size()) : 1;
                List<String> popped = new ArrayList<>(list.subList(0, count));
                list.subList(0, count).clear();
                if (list.isEmpty()) lists.remove(command.get(1));
                yield multiple ? array(popped) : bulk(popped.get(0));
            }
            case "BLPOP" -> {
                String key = command.get(1);
                List<String> list = lists.get(key);
                if (list != null && !list.isEmpty()) {
                    String value = list.remove(0);
                    if (list.isEmpty()) lists.remove(key);
                    yield array(List.of(key, value));
                }
                if (executing) yield "*-1\r\n";
                double seconds = Double.parseDouble(command.get(2));
                long deadline = seconds == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + (long) (seconds * 1000);
                blocked.computeIfAbsent(key, k -> new ArrayDeque<>()).add(new Waiter(client, deadline));
                c.blocked = true;
                yield null;
            }
            case "LRANGE" -> {
                List<String> list = lists.getOrDefault(command.get(1), List.of());
                int start = Math.max(0, index(Integer.parseInt(command.get(2)), list.size()));
                int stop = Math.min(index(Integer.parseInt(command.get(3)), list.size()), list.size() - 1);
                yield array(start > stop ? List.of() : list.subList(start, stop + 1));
            }
            default -> "-ERR unknown command '" + command.get(0) + "'\r\n";
        };
    }

    static List<Object> xread(List<String> keys, List<long[]> after) {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            List<Object> entries = new ArrayList<>();
            for (Entry entry : streams.getOrDefault(keys.get(i), List.of())) {
                if (compare(entry, after.get(i)) > 0) entries.add(List.of(entry.id(), entry.fields()));
            }
            if (!entries.isEmpty()) result.add(List.of(keys.get(i), entries));
        }
        return result;
    }

    static void wake(String key) throws IOException {
        Iterator<Reader> it = readers.iterator();
        while (it.hasNext()) {
            Reader reader = it.next();
            if (!reader.keys().contains(key)) continue;
            List<Object> result = xread(reader.keys(), reader.after());
            if (result.isEmpty()) continue;
            it.remove();
            if (reader.client().isOpen()) answer(reader.client(), array(result));
        }
    }

    static void serve(String key) throws IOException {
        Deque<Waiter> waiting = blocked.getOrDefault(key, new ArrayDeque<>());
        List<String> list = lists.get(key);
        while (!waiting.isEmpty() && !list.isEmpty()) {
            SocketChannel waiter = waiting.poll().client();
            if (waiter.isOpen() && answer(waiter, array(List.of(key, list.get(0))))) list.remove(0);
        }
        if (list.isEmpty()) lists.remove(key);
        if (waiting.isEmpty()) blocked.remove(key);
    }

    static long nextTimeout() {
        long next = Long.MAX_VALUE;
        for (Deque<Waiter> waiting : blocked.values()) {
            for (Waiter waiter : waiting) next = Math.min(next, waiter.deadline());
        }
        for (Reader reader : readers) next = Math.min(next, reader.deadline());
        return next == Long.MAX_VALUE ? 0 : Math.max(1, next - System.currentTimeMillis());
    }

    static void expire() throws IOException {
        long now = System.currentTimeMillis();
        Iterator<Deque<Waiter>> queues = blocked.values().iterator();
        while (queues.hasNext()) {
            Deque<Waiter> waiting = queues.next();
            Iterator<Waiter> waiters = waiting.iterator();
            while (waiters.hasNext()) {
                Waiter waiter = waiters.next();
                if (now >= waiter.deadline()) {
                    waiters.remove();
                    if (waiter.client().isOpen()) answer(waiter.client(), "*-1\r\n");
                }
            }
            if (waiting.isEmpty()) queues.remove();
        }
        Iterator<Reader> it = readers.iterator();
        while (it.hasNext()) {
            Reader reader = it.next();
            if (now >= reader.deadline()) {
                it.remove();
                if (reader.client().isOpen()) answer(reader.client(), "*-1\r\n");
            }
        }
    }

    // Answers a blocked client (served, or timed out) and unblocks it. The reply goes into its output buffer now; the
    // commands it sent while blocked run later in this loop pass, so their replies follow it. Returns false if the
    // client is already gone.
    static boolean answer(SocketChannel client, String response) {
        SelectionKey key = client.keyFor(selector);
        if (key == null || !key.isValid()) return false;
        Connection connection = (Connection) key.attachment();
        write(connection, response);
        connection.blocked = false;
        pending.add(key);
        resumed.add(key);
        return true;
    }

    static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] PONG = "+PONG\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] NIL = "$-1\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.US_ASCII);
    static final String NOT_AN_INTEGER = "-ERR value is not an integer or out of range\r\n";

    // The client's output buffer with room for at least needed more bytes; it doubles when full and never shrinks.
    static ByteBuffer room(Connection c, int needed) {
        ByteBuffer out = c.out;
        if (out.remaining() < needed) {
            int size = out.capacity();
            while (size - out.position() < needed) size *= 2;
            out = c.out = ByteBuffer.allocate(size).put(out.flip());
        }
        return out;
    }

    static void write(Connection c, String reply) {
        byte[] bytes = reply.getBytes(StandardCharsets.UTF_8);
        room(c, bytes.length).put(bytes);
    }

    static void writeBytes(Connection c, byte[] bytes) {
        room(c, bytes.length).put(bytes);
    }

    static void writeBulk(Connection c, byte[] a, int offset, int length) {
        ByteBuffer out = room(c, length + 24);
        out.put((byte) '$');
        putNumber(out, length);
        out.put(CRLF).put(a, offset, length).put(CRLF);
    }

    static void writeInteger(Connection c, long value) {
        ByteBuffer out = room(c, 24);
        out.put((byte) ':');
        putNumber(out, value);
        out.put(CRLF);
    }

    // Writes a decimal number as ASCII digits without making a String.
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

    // Writes as much of the client's queued replies as the socket takes. While some are left, the client is watched for
    // OP_WRITE instead of OP_READ: it gets no new commands read until it has read its replies, which bounds the buffer.
    static void flush(SelectionKey key) {
        if (!key.isValid()) return;
        Connection connection = (Connection) key.attachment();
        SocketChannel client = (SocketChannel) key.channel();
        connection.out.flip();
        try {
            client.write(connection.out);
        } catch (IOException e) {
            connection.out.clear();
            disconnect(client);
            return;
        }
        connection.out.compact();
        key.interestOps(connection.out.position() > 0 ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);
    }

    static long[] parseId(String id, long defaultSeq) {
        if (id.equals("-")) return new long[] {0, 0};
        if (id.equals("+")) return new long[] {Long.MAX_VALUE, Long.MAX_VALUE};
        String[] parts = id.split("-");
        return new long[] {Long.parseLong(parts[0]), parts.length > 1 ? Long.parseLong(parts[1]) : defaultSeq};
    }

    static long[] lastId(String key) {
        List<Entry> stream = streams.getOrDefault(key, List.of());
        if (stream.isEmpty()) return new long[] {0, 0};
        Entry last = stream.get(stream.size() - 1);
        return new long[] {last.ms(), last.seq()};
    }

    static int compare(Entry entry, long[] id) {
        return entry.ms() != id[0] ? Long.compare(entry.ms(), id[0]) : Long.compare(entry.seq(), id[1]);
    }

    static long nextSeq(Entry last, long ms) {
        if (last != null && last.ms() == ms) return last.seq() + 1;
        return ms == 0 ? 1 : 0;
    }

    static String type(String key) {
        if (get(Key.of(key)) != null) return "string";
        if (lists.containsKey(key)) return "list";
        if (streams.containsKey(key)) return "stream";
        return "none";
    }

    // The value of a string key, deleting it first if its time is up (expiry is still lazy, as in v1).
    static byte[] get(Key key) {
        if (!expiries.isEmpty()) {
            Long expiresAt = expiries.get(key);
            if (expiresAt != null && System.currentTimeMillis() >= expiresAt) {
                store.remove(key);
                expiries.remove(key);
            }
        }
        return store.get(key);
    }

    static String bulk(String value) {
        return "$" + value.getBytes(StandardCharsets.UTF_8).length + "\r\n" + value + "\r\n";
    }

    static int index(int value, int size) {
        return value < 0 ? size + value : value;
    }

    static String array(List<?> values) {
        StringBuilder out = new StringBuilder("*" + values.size() + "\r\n");
        for (Object value : values) out.append(value instanceof List<?> list ? array(list) : bulk((String) value));
        return out.toString();
    }
}
