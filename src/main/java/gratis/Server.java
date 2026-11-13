package gratis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;

/** Starts the server: options, the listening socket, and the event loop. */
public final class Server {
    private Server() {
    }

    public static void main(String[] args) throws IOException {
        int port = 6379;
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);
        }
        Shard shard = new Shard();
        ServerSocketChannel listener = ServerSocketChannel.open();
        listener.bind(new InetSocketAddress(port));
        listener.configureBlocking(false);
        listener.register(shard.selector, SelectionKey.OP_ACCEPT);
        System.out.println("Gratis listening on port " + port);
        shard.run();
    }
}
