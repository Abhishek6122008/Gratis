package gratis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One event loop: the clients it accepted, and the keys whose slot maps to it. Nothing here is shared with other
 * shards. A client's command for a key on another shard goes there as a run (that command plus any that follow it for
 * the same shard). The client waits, with its later commands unparsed, until the run's replies come back, so one
 * client's commands take effect in the order it sent them. A command whose keys are on several shards (an EXEC, or
 * XREAD over several streams) runs with every other shard paused.
 */
final class Shard implements Runnable {
    // What owner() returns for a command with no keys, and for one whose keys are on more than one shard.
    static final int ANY = -1;
    static final int SPLIT = -2;
    static final String BLOCK_ACROSS_SHARDS =
            "-ERR XREAD BLOCK needs all its streams on one shard: give their names a common {hash tag}\r\n";

    static final byte[] OK = "+OK\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] PONG = "+PONG\r\n".getBytes(StandardCharsets.US_ASCII);
    static final byte[] NIL = "$-1\r\n".getBytes(StandardCharsets.US_ASCII);
    static final String NOT_AN_INTEGER = "-ERR value is not an integer or out of range\r\n";
    // Redis's limit for one bulk string; a request still incomplete at this size ends the client.
    static final int MAX_REQUEST = 512 * 1024 * 1024;

    final int id;
    final Selector selector;
    private Shard[] all;
    // Held by the shard running a command across shards while the others are paused.
    private ReentrantLock across;

    // String keys and values are bytes, so any value round-trips unchanged (v1 decoded them as UTF-8 text).
    final Map<Key, byte[]> store = new HashMap<>();
    final Map<Key, Long> expiries = new HashMap<>();
    final Map<String, List<String>> lists = new HashMap<>();
    final Map<String, Deque<Waiter>> blocked = new HashMap<>();
    final Map<String, List<Entry>> streams = new HashMap<>();
    final List<Reader> readers = new ArrayList<>();
    boolean executing;

    // Clients with replies waiting in their output buffer, flushed once per loop pass.
    private final List<Connection> pending = new ArrayList<>();
    // Clients unblocked in this loop pass, or whose run came back, whose pipelined commands are still to run.
    private final Deque<Connection> resumed = new ArrayDeque<>();

    // Connections handed over by other shards: a run to execute here, or one of this shard's clients whose run is done.
    // A lock-free stack; a connection is in at most one inbox at a time, so its own `next` field links it.
    private final AtomicReference<Connection> inbox = new AtomicReference<>();
    // New clients from the accepting shard.
    private final ConcurrentLinkedQueue<Runnable> tasks = new ConcurrentLinkedQueue<>();
    // Shards this pass handed work to while they slept, woken once at the end of the pass.
    private boolean[] wake;
    // True while this shard is (about to be) asleep in select(). Under load shards are rarely asleep, so most hand-offs
    // cost no wakeup system call.
    private volatile boolean sleeping;
    // Arguments of the run being executed here for another shard's client.
    private final Args remote = new Args();
    private int nextShard;
    // Clients whose command across shards found another shard already running one: tried again next pass.
    private final List<Connection> retry = new ArrayList<>();

    record Waiter(Connection client, long deadline) {}

    record Reader(Connection client, List<String> keys, List<long[]> after, long deadline) {}

    record Entry(long ms, long seq, List<String> fields) {
        String id() {
            return ms + "-" + seq;
        }
    }

    Shard(int id) throws IOException {
        this.id = id;
        selector = Selector.open();
    }

    void join(Shard[] shards, ReentrantLock acrossShards) {
        all = shards;
        across = acrossShards;
        wake = new boolean[shards.length];
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (retry.isEmpty()) {
                    // Announce the sleep, then look once more: a shard that pushed before seeing `sleeping` left its
                    // work in the inbox, and one that pushed after will wake us (both are volatile, so one must hold).
                    sleeping = true;
                    if (inbox.get() == null && tasks.isEmpty()) selector.select(this::ready, nextTimeout());
                    else selector.selectNow(this::ready);
                    sleeping = false;
                } else {
                    selector.selectNow(this::ready);
                    for (Connection c : retry) {
                        c.waiting = false;
                        resumed.add(c);
                    }
                    retry.clear();
                }
                for (Runnable task; (task = tasks.poll()) != null; ) task.run();
                drainInbox();
                expire();
                // Clients answered in this pass run what they sent after the blocking command. That can answer others.
                while (!resumed.isEmpty()) {
                    Connection c = resumed.poll();
                    if (!c.key.isValid()) continue;
                    try {
                        process(c);
                    } catch (IOException e) {
                        close(c);
                    }
                }
                for (Connection c : pending) {
                    c.pending = false;
                    flush(c);
                }
                pending.clear();
                for (int i = 0; i < wake.length; i++) {
                    if (wake[i]) {
                        wake[i] = false;
                        all[i].selector.wakeup();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void ready(SelectionKey key) {
        if (key.isAcceptable()) {
            accept((ServerSocketChannel) key.channel());
            return;
        }
        Connection c = (Connection) key.attachment();
        if (key.isValid() && key.isWritable()) markPending(c);
        if (key.isValid() && key.isReadable()) {
            try {
                read(c);
            } catch (IOException e) {
                close(c);
            }
        }
    }

    private void accept(ServerSocketChannel listener) {
        try {
            SocketChannel channel = listener.accept();
            if (channel == null) return;
            channel.configureBlocking(false);
            Shard target = all[nextShard];
            nextShard = (nextShard + 1) % all.length;
            if (target == this) {
                register(channel);
            } else {
                target.tasks.add(() -> target.register(channel));
                target.selector.wakeup();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void register(SocketChannel channel) {
        try {
            Connection c = new Connection(this, channel);
            c.key = channel.register(selector, SelectionKey.OP_READ, c);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void markPending(Connection c) {
        if (!c.pending) {
            c.pending = true;
            pending.add(c);
        }
    }

    // One client's failed read must not stop the server for everyone else, so its IOException ends only that client.
    private void read(Connection c) throws IOException {
        ByteBuffer buffer = c.in;
        if (!buffer.hasRemaining()) {
            // The buffer holds one incomplete command that fills it: make room for the rest.
            if (buffer.capacity() >= MAX_REQUEST) throw new IOException("request too large");
            buffer = c.in = ByteBuffer.allocate(Math.min(MAX_REQUEST, buffer.capacity() * 2)).put(buffer.flip());
        }
        if (c.channel.read(buffer) == -1) {
            close(c);
            return;
        }
        process(c);
    }

    // Runs every complete command in the client's input buffer, here or as a run on the shard that owns its key. A
    // command that blocks, or a run out at another shard, stops the loop: like Redis, the client's later commands wait,
    // so its replies stay in the order it sent the commands. Its replies are written once nothing is out, which keeps
    // one write per pipeline.
    private void process(Connection c) throws IOException {
        ByteBuffer buffer = c.in.flip();
        Args args = c.args;
        while (!c.blocked && !c.waiting) {
            int start = buffer.position();
            if (!Resp.parse(buffer, args)) break;
            if (args.size() == 0) continue;
            int owner;
            if (c.queue == null) owner = owner(args);
            else owner = args.is(0, "EXEC") ? owner(c.queue) : ANY; // inside MULTI, commands are queued here
            if (owner == ANY || owner == id) {
                run(args, c, c.out);
            } else if (owner == SPLIT) {
                if (!alone(() -> runAcross(args, c))) {
                    buffer.position(start); // another shard is running one: try this command again next pass
                    c.waiting = true;
                    retry.add(c);
                }
            } else if (c.queue != null) {
                c.exec = c.queue;   // EXEC: the shard that owns every key runs the queued commands
                c.queue = null;
                send(c, owner);
            } else {
                dispatch(c, owner, buffer, start, args);
            }
        }
        buffer.compact();
        if (c.out.position() > 0 && !c.waiting) markPending(c);
    }

    // Runs a command whose keys are on several shards (only called while every other shard is paused): an EXEC runs
    // each queued command on the shard that owns its keys; an XREAD reads each stream where it lives.
    private void runAcross(Args args, Connection c) {
        if (!args.is(0, "EXEC")) {
            run(args, c, c.out);
            return;
        }
        List<Args> queued = c.queue;
        c.queue = null;
        c.out.put("*" + queued.size() + "\r\n");
        for (Args command : queued) {
            int owner = owner(command);
            Shard data = owner >= 0 ? all[owner] : this;
            data.executing = true;
            try {
                data.run(command, c, c.out);
            } finally {
                data.executing = false;
            }
        }
    }

    // Runs task with every other shard paused, so it can read and change keys on any of them as one step. Each paused
    // shard is parked in a task on its own loop, between passes. Returns false, without running the task, if another
    // shard is already doing this; the caller tries again on its next pass and is paused meanwhile, so no shard ever
    // waits on another while holding anything.
    private boolean alone(Runnable task) {
        if (!across.tryLock()) return false;
        try {
            CountDownLatch paused = new CountDownLatch(all.length - 1);
            CountDownLatch resume = new CountDownLatch(1);
            for (Shard other : all) {
                if (other == this) continue;
                other.tasks.add(() -> {
                    paused.countDown();
                    awaitUninterruptibly(resume);
                });
                other.selector.wakeup();
            }
            awaitUninterruptibly(paused);
            try {
                task.run();
            } finally {
                resume.countDown();
            }
        } finally {
            across.unlock();
        }
        return true;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    // The shard that owns the command's keys: ANY if it has none, SPLIT if they are on more than one shard.
    int owner(Args args) {
        if (all.length == 1 || args.size() < 2) return ANY;
        if (args.is(0, "PING") || args.is(0, "ECHO") || args.is(0, "MULTI") || args.is(0, "EXEC")
                || args.is(0, "DISCARD")) {
            return ANY;
        }
        if (!args.is(0, "XREAD")) return shardOf(args, 1);
        int streams = 1;
        while (streams < args.size() && !args.is(streams, "STREAMS")) streams++;
        int keys = (args.size() - streams - 1) / 2;
        int owner = ANY;
        for (int i = streams + 1; i <= streams + keys; i++) owner = combine(owner, shardOf(args, i));
        return owner;
    }

    // The shard that owns every key in a MULTI queue.
    int owner(List<Args> queue) {
        int owner = ANY;
        for (Args command : queue) owner = combine(owner, owner(command));
        return owner;
    }

    private static int combine(int a, int b) {
        if (a == ANY) return b;
        if (b == ANY) return a;
        return a == b ? a : SPLIT;
    }

    private int shardOf(Args args, int i) {
        return Slots.slot(args.array(i), args.offset(i), args.length(i)) % all.length;
    }

    private static boolean mayBlock(Args args) {
        return args.is(0, "BLPOP") || args.is(0, "XREAD");
    }

    // Sends a run to its shard: this command and the ones after it for the same shard, up to one that may block. The
    // run is the commands' raw bytes, copied, because the input buffer moves on.
    private void dispatch(Connection c, int owner, ByteBuffer buffer, int start, Args args) throws IOException {
        Out request = c.request;
        request.buf.clear();
        request.put(buffer.array(), buffer.arrayOffset() + start, buffer.position() - start);
        while (!mayBlock(args)) {
            int next = buffer.position();
            if (!Resp.parse(buffer, args)) break;
            if (args.size() == 0) continue;
            if (owner(args) != owner) {
                buffer.position(next);
                break;
            }
            request.put(buffer.array(), buffer.arrayOffset() + next, buffer.position() - next);
        }
        send(c, owner);
    }

    // Hands the client's run to its shard. The client waits until the run comes back.
    private void send(Connection c, int owner) {
        c.waiting = true;
        c.reply.buf.clear();
        handOver(c, all[owner]);
    }

    // Puts a client in another shard's inbox, and wakes that shard at the end of this pass if it is asleep.
    private void handOver(Connection c, Shard to) {
        Connection head;
        do {
            head = to.inbox.get();
            c.next = head;
        } while (!to.inbox.compareAndSet(head, c));
        if (to.sleeping) wake[to.id] = true;
    }

    private void drainInbox() {
        Connection stack = inbox.getAndSet(null);
        Connection first = null;
        while (stack != null) { // reverse, to arrival order
            Connection next = stack.next;
            stack.next = first;
            first = stack;
            stack = next;
        }
        while (first != null) {
            Connection c = first;
            first = c.next;
            c.next = null;
            if (c.home == this) complete(c);
            else execute(c);
        }
    }

    // Runs another shard's client's run here, writing the replies into its reply buffer, then sends it home. A run
    // that ends with a command that blocks stays here until answer() sends it home.
    private void execute(Connection c) {
        Out reply = c.reply;
        List<Args> transaction = c.exec;
        if (transaction != null) {
            c.exec = null;
            reply.put("*" + transaction.size() + "\r\n");
            executing = true;
            try {
                for (Args command : transaction) run(command, c, reply);
            } finally {
                executing = false;
            }
        } else {
            ByteBuffer request = c.request.buf.flip();
            try {
                while (Resp.parse(request, remote)) {
                    if (remote.size() == 0) continue;
                    run(remote, c, reply);
                    if (c.blocked) return;
                }
            } catch (Resp.ProtocolException e) {
                throw new IllegalStateException("a run that parsed at home did not parse here", e);
            }
        }
        sendHome(c);
    }

    private void sendHome(Connection c) {
        if (c.home == this) complete(c);
        else handOver(c, c.home);
    }

    // One of this shard's clients is back from a run: its replies go out after the ones before them, and its next
    // commands can run.
    private void complete(Connection c) {
        c.waiting = false;
        if (!c.key.isValid()) return; // it went away while the run was out
        ByteBuffer reply = c.reply.buf.flip();
        c.out.put(reply.array(), reply.arrayOffset(), reply.limit());
        reply.clear();
        resumed.add(c);
    }

    private void close(Connection c) {
        try {
            c.channel.close();
        } catch (IOException ignored) {
            // closing anyway
        }
    }

    // Runs one command and puts its reply in the client's output buffer. A command that throws gets an error reply, the
    // way Redis answers bad arguments, instead of ending the loop; anything it wrote before throwing is dropped.
    void run(Args args, Connection c, Out out) {
        int mark = out.position();
        String reply;
        try {
            reply = handle(args, c, out);
        } catch (IndexOutOfBoundsException e) {
            out.position(mark);
            reply = "-ERR wrong number of arguments for '" + args.string(0).toLowerCase() + "' command\r\n";
        } catch (NumberFormatException e) {
            out.position(mark);
            reply = NOT_AN_INTEGER;
        } catch (RuntimeException e) {
            out.position(mark);
            reply = "-ERR " + e.getMessage() + "\r\n";
        }
        if (reply != null) out.put(reply);
    }

    // The string commands are the hot path (memtier sends only SET and GET). They read their arguments as bytes, write
    // their reply straight into the output buffer and return null. Every other command goes through handleOther with
    // its arguments as Strings and returns its reply, or null when the client is now blocked.
    String handle(Args args, Connection c, Out out) {
        if (c.queue != null && !args.is(0, "MULTI") && !args.is(0, "EXEC") && !args.is(0, "DISCARD")) {
            c.queue.add(args.copy());
            return "+QUEUED\r\n";
        }
        if (args.is(0, "GET")) {
            byte[] value = get(args.key(1));
            if (value == null) out.put(NIL);
            else out.putBulk(value, 0, value.length);
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
            out.put(OK);
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
            out.putInteger(value);
            return null;
        }
        if (args.is(0, "PING")) {
            out.put(PONG);
            return null;
        }
        if (args.is(0, "ECHO")) {
            args.check(1);
            out.putBulk(args.array(1), args.offset(1), args.length(1));
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
            out.put("*" + queued.size() + "\r\n");
            executing = true;
            try {
                for (Args command : queued) run(command, c, out);
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
        return handleOther(args.strings(), c);
    }

    String handleOther(List<String> command, Connection c) {
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
                for (String key : keys) {
                    if (ownerOf(key) != this) yield BLOCK_ACROSS_SHARDS;
                }
                long deadline = block == 0 ? Long.MAX_VALUE : System.currentTimeMillis() + block;
                readers.add(new Reader(c, List.copyOf(keys), after, deadline));
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
                blocked.computeIfAbsent(key, k -> new ArrayDeque<>()).add(new Waiter(c, deadline));
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

    List<Object> xread(List<String> keys, List<long[]> after) {
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            List<Object> entries = new ArrayList<>();
            for (Entry entry : stream(keys.get(i))) {
                if (compare(entry, after.get(i)) > 0) entries.add(List.of(entry.id(), entry.fields()));
            }
            if (!entries.isEmpty()) result.add(List.of(keys.get(i), entries));
        }
        return result;
    }

    void wake(String key) {
        Iterator<Reader> it = readers.iterator();
        while (it.hasNext()) {
            Reader reader = it.next();
            if (!reader.keys().contains(key)) continue;
            List<Object> result = xread(reader.keys(), reader.after());
            if (result.isEmpty()) continue;
            it.remove();
            answer(reader.client(), array(result));
        }
    }

    void serve(String key) {
        Deque<Waiter> waiting = blocked.getOrDefault(key, new ArrayDeque<>());
        List<String> list = lists.get(key);
        while (!waiting.isEmpty() && !list.isEmpty()) {
            Connection waiter = waiting.poll().client();
            if (answer(waiter, array(List.of(key, list.get(0))))) list.remove(0);
        }
        if (list.isEmpty()) lists.remove(key);
        if (waiting.isEmpty()) blocked.remove(key);
    }

    long nextTimeout() {
        long next = Long.MAX_VALUE;
        for (Deque<Waiter> waiting : blocked.values()) {
            for (Waiter waiter : waiting) next = Math.min(next, waiter.deadline());
        }
        for (Reader reader : readers) next = Math.min(next, reader.deadline());
        return next == Long.MAX_VALUE ? 0 : Math.max(1, next - System.currentTimeMillis());
    }

    void expire() {
        long now = System.currentTimeMillis();
        Iterator<Deque<Waiter>> queues = blocked.values().iterator();
        while (queues.hasNext()) {
            Deque<Waiter> waiting = queues.next();
            Iterator<Waiter> waiters = waiting.iterator();
            while (waiters.hasNext()) {
                Waiter waiter = waiters.next();
                if (now >= waiter.deadline()) {
                    waiters.remove();
                    answer(waiter.client(), "*-1\r\n");
                }
            }
            if (waiting.isEmpty()) queues.remove();
        }
        Iterator<Reader> it = readers.iterator();
        while (it.hasNext()) {
            Reader reader = it.next();
            if (now >= reader.deadline()) {
                it.remove();
                answer(reader.client(), "*-1\r\n");
            }
        }
    }

    // Answers a blocked client (served, or timed out) and unblocks it. The reply goes into its output buffer now; the
    // commands it sent while blocked run later in this loop pass, so their replies follow it. Returns false if the
    // client is already gone.
    boolean answer(Connection client, String response) {
        if (!client.channel.isOpen()) return false;
        client.blocked = false;
        if (client.home == this) {
            client.out.put(response);
            markPending(client);
            resumed.add(client);
        } else {
            client.reply.put(response); // after the replies of the commands before it in its run
            sendHome(client);
        }
        return true;
    }

    // Writes as much of the client's queued replies as the socket takes. While some are left, the client is watched for
    // OP_WRITE instead of OP_READ: it gets no new commands read until it has read its replies, which bounds the buffer.
    private void flush(Connection c) {
        if (!c.key.isValid()) return;
        ByteBuffer out = c.out.buf.flip();
        try {
            c.channel.write(out);
        } catch (IOException e) {
            out.clear();
            close(c);
            return;
        }
        out.compact();
        c.key.interestOps(out.position() > 0 ? SelectionKey.OP_WRITE : SelectionKey.OP_READ);
    }

    static long[] parseId(String id, long defaultSeq) {
        if (id.equals("-")) return new long[] {0, 0};
        if (id.equals("+")) return new long[] {Long.MAX_VALUE, Long.MAX_VALUE};
        String[] parts = id.split("-");
        return new long[] {Long.parseLong(parts[0]), parts.length > 1 ? Long.parseLong(parts[1]) : defaultSeq};
    }

    // The shard that owns a key. Commands only touch keys on their own shard, except while running across shards.
    Shard ownerOf(String key) {
        return all.length == 1 ? this : all[Slots.slot(key.getBytes(StandardCharsets.UTF_8)) % all.length];
    }

    List<Entry> stream(String key) {
        return ownerOf(key).streams.getOrDefault(key, List.of());
    }

    long[] lastId(String key) {
        List<Entry> stream = stream(key);
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

    String type(String key) {
        if (get(Key.of(key)) != null) return "string";
        if (lists.containsKey(key)) return "list";
        if (streams.containsKey(key)) return "stream";
        return "none";
    }

    // The value of a string key, deleting it first if its time is up (expiry is still lazy, as in v1).
    byte[] get(Key key) {
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
