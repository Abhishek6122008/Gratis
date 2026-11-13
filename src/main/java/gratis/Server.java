package gratis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;

/**
 * Starts the server: options, the shards (one event loop thread each), and the listening socket. Shard 0 accepts
 * connections and hands them out in turn.
 *
 * <p>Options: {@code --port N} (default 6379), {@code --shards N} (default: one per CPU the JVM may use).
 */
public final class Server {
    private Server() {
    }

    public static void main(String[] args) throws IOException {
        int port = 6379;
        int count = Runtime.getRuntime().availableProcessors();
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
            if (args[i].equals("--shards")) count = Integer.parseInt(args[i + 1]);
        }
        if (count < 1) throw new IllegalArgumentException("--shards must be at least 1");
        Shard[] shards = new Shard[count];
        for (int i = 0; i < count; i++) shards[i] = new Shard(i);
        for (Shard shard : shards) shard.join(shards);
        ServerSocketChannel listener = ServerSocketChannel.open();
        listener.bind(new InetSocketAddress(port));
        listener.configureBlocking(false);
        listener.register(shards[0].selector, SelectionKey.OP_ACCEPT);
        for (int i = 1; i < count; i++) Thread.ofPlatform().name("shard-" + i).start(shards[i]);
        System.out.println("Gratis listening on port " + port + " with " + count + " shard" + (count == 1 ? "" : "s"));
        Thread.currentThread().setName("shard-0");
        shards[0].run();
    }
}
