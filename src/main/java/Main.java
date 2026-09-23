import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class Main {
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

                        client.register(selector, SelectionKey.OP_READ);

                        System.out.println("Client connected!");
                    }
                    if (key.isReadable()) {
                        SocketChannel client = (SocketChannel) key.channel();
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        int bytesRead = client.read(buffer);
                        if (bytesRead == -1) {
                            client.close();
                            key.cancel();
                            System.out.println("Client disconnected!");
                            continue;
                        }
                        String command = new String(
                                buffer.array(),
                                0,
                                bytesRead,
                                StandardCharsets.UTF_8
                        );
                        System.out.println("Received: " + command);
                        ByteBuffer response = ByteBuffer.wrap(
                                "+PONG\r\n".getBytes(StandardCharsets.UTF_8)
                        );
                        client.write(response);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}