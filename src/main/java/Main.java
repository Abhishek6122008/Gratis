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
import java.util.List;
import java.util.Map;

public class Main {
    static final Map<String, String> store = new HashMap<>();
    static final Map<String, Long> expiries = new HashMap<>();
    static final Map<String, List<String>> lists = new HashMap<>();
    static final Map<String, Deque<Waiter>> blocked = new HashMap<>();

    static final Map<String, List<Entry>> streams = new HashMap<>();
    static final List<Reader> readers = new ArrayList<>();

    record Waiter(SocketChannel client, long deadline) {}

    record Reader(SocketChannel client, List<String> keys, List<long[]> after, long deadline) {}

    record Entry(long ms, long seq, List<String> fields) {
        String id() {
            return ms + "-" + seq;
        }
    }

    public static void main(String[] args) {
        System.out.println("Redis server started on port 6379");
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(6379));
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
                        client.configureBlocking(false);
                        client.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(64 * 1024));
                        System.out.println("Client connected!");
                    }
                    if (key.isValid() && key.isReadable()) {
                        SocketChannel client = (SocketChannel) key.channel();
                        ByteBuffer buffer = (ByteBuffer) key.attachment();
                        if (client.read(buffer) == -1) {
                            client.close();
                            key.cancel();
                            System.out.println("Client disconnected!");
                            continue;
                        }
                        buffer.flip();
                        List<String> command;
                        while ((command = parse(buffer)) != null) {
                            String response = handle(command, client);
                            if (response != null) send(client, response);
                        }
                        buffer.compact();
                    }
                }
                expire();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String handle(List<String> command, SocketChannel client) throws IOException {
        return switch (command.get(0).toUpperCase()) {
            case "PING" -> "+PONG\r\n";
            case "ECHO" -> bulk(command.get(1));
            case "SET" -> {
                String key = command.get(1);
                store.put(key, command.get(2));
                expiries.remove(key);
                for (int i = 3; i + 1 < command.size(); i += 2) {
                    long amount = Long.parseLong(command.get(i + 1));
                    switch (command.get(i).toUpperCase()) {
                        case "EX" -> expiries.put(key, System.currentTimeMillis() + amount * 1000);
                        case "PX" -> expiries.put(key, System.currentTimeMillis() + amount);
                    }
                }
                yield "+OK\r\n";
            }
            case "GET" -> {
                String value = get(command.get(1));
                yield value == null ? "$-1\r\n" : bulk(value);
            }
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
                for (int i = 0; i < n; i++) after.add(parseId(command.get(s + 1 + n + i), 0));
                List<Object> result = xread(keys, after);
                if (!result.isEmpty()) yield array(result);
                if (block < 0) yield "*-1\r\n";
                long deadline = block == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + block;
                readers.add(new Reader(client, List.copyOf(keys), after, deadline));
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
                double seconds = Double.parseDouble(command.get(2));
                long deadline = seconds == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + (long) (seconds * 1000);
                blocked.computeIfAbsent(key, k -> new ArrayDeque<>()).add(new Waiter(client, deadline));
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
            if (reader.client().isOpen()) send(reader.client(), array(result));
        }
    }

    static void serve(String key) throws IOException {
        Deque<Waiter> waiting = blocked.getOrDefault(key, new ArrayDeque<>());
        List<String> list = lists.get(key);
        while (!waiting.isEmpty() && !list.isEmpty()) {
            SocketChannel waiter = waiting.poll().client();
            if (waiter.isOpen()) send(waiter, array(List.of(key, list.remove(0))));
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
                    if (waiter.client().isOpen()) send(waiter.client(), "*-1\r\n");
                }
            }
            if (waiting.isEmpty()) queues.remove();
        }
        Iterator<Reader> it = readers.iterator();
        while (it.hasNext()) {
            Reader reader = it.next();
            if (now >= reader.deadline()) {
                it.remove();
                if (reader.client().isOpen()) send(reader.client(), "*-1\r\n");
            }
        }
    }

    static void send(SocketChannel client, String response) throws IOException {
        client.write(ByteBuffer.wrap(response.getBytes(StandardCharsets.UTF_8)));
    }

    static long[] parseId(String id, long defaultSeq) {
        if (id.equals("-")) return new long[] {0, 0};
        if (id.equals("+")) return new long[] {Long.MAX_VALUE, Long.MAX_VALUE};
        String[] parts = id.split("-");
        return new long[] {Long.parseLong(parts[0]), parts.length > 1 ? Long.parseLong(parts[1]) : defaultSeq};
    }

    static int compare(Entry entry, long[] id) {
        return entry.ms() != id[0] ? Long.compare(entry.ms(), id[0]) : Long.compare(entry.seq(), id[1]);
    }

    static long nextSeq(Entry last, long ms) {
        if (last != null && last.ms() == ms) return last.seq() + 1;
        return ms == 0 ? 1 : 0;
    }

    static String type(String key) {
        if (get(key) != null) return "string";
        if (lists.containsKey(key)) return "list";
        if (streams.containsKey(key)) return "stream";
        return "none";
    }

    static String get(String key) {
        Long expiresAt = expiries.get(key);
        if (expiresAt != null && System.currentTimeMillis() >= expiresAt) {
            store.remove(key);
            expiries.remove(key);
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
