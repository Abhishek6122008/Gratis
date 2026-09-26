import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Main {
    static final Map<String, String> store = new HashMap<>();
    static final Map<String, Long> expiries = new HashMap<>();
    static final Map<String, List<String>> lists = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("Redis server started on port 6379");
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverChannel = ServerSocketChannel.open();
            serverChannel.bind(new InetSocketAddress(6379));
            serverChannel.configureBlocking(false);
            serverChannel.register(selector, SelectionKey.OP_ACCEPT);
            while (true) {
                selector.select();
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
                            client.write(ByteBuffer.wrap(handle(command).getBytes(StandardCharsets.UTF_8)));
                        }
                        buffer.compact();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String handle(List<String> command) {
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
            case "RPUSH" -> {
                List<String> list = lists.computeIfAbsent(command.get(1), k -> new ArrayList<>());
                list.addAll(command.subList(2, command.size()));
                yield ":" + list.size() + "\r\n";
            }
            default -> "-ERR unknown command '" + command.get(0) + "'\r\n";
        };
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
