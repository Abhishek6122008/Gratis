package gratis;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;

/** One client: its socket, its unparsed input, and the replies not yet written to it. Owned by its home shard. */
final class Connection {
    final Shard home;
    final SocketChannel channel;
    SelectionKey key;
    ByteBuffer in = ByteBuffer.allocate(64 * 1024);
    final Out out = new Out(16 * 1024);
    final Args args = new Args();
    // The commands queued after MULTI, or null outside a transaction.
    List<Args> queue;
    // Waiting in BLPOP or XREAD BLOCK: commands it sends meanwhile stay unparsed in `in` until it is answered.
    boolean blocked;
    // On its shard's list of clients to flush at the end of this loop pass.
    boolean pending;

    Connection(Shard home, SocketChannel channel) {
        this.home = home;
        this.channel = channel;
    }
}
