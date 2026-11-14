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

    // A run out at another shard (see Shard). While `waiting`, the home shard leaves the client alone: the owning shard
    // reads `request` (or `exec`, an EXEC's queued commands) and writes `reply`, then hands the client back. The
    // inboxes between shards order those writes before the reads.
    boolean waiting;
    final Out request = new Out(1024);
    final Out reply = new Out(1024);
    List<Args> exec;
    Connection next;

    Connection(Shard home, SocketChannel channel) {
        this.home = home;
        this.channel = channel;
    }
}
