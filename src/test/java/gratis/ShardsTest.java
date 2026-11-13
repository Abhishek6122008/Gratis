package gratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

/**
 * Sharding (every ServerTest runs 4 shards). Clients are handed to shards in turn, and keys go to the shard their slot
 * maps to, so these tests pick keys on every shard and use several clients to cross between shards both ways.
 */
class ShardsTest extends ServerTest {
    private static final int SHARDS = 4;


    /** A key starting with prefix that lives on the given shard. */
    static String keyOn(int shard, String prefix) {
        for (int i = 0; ; i++) {
            String key = prefix + i;
            if (Slots.slot(key.getBytes(StandardCharsets.UTF_8)) % SHARDS == shard) return key;
        }
    }

    private static byte[] pipeline(List<String[]> commands) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String[] command : commands) out.writeBytes(RespClient.encode(command));
        return out.toByteArray();
    }

    @Test
    void aPipelineOverKeysOnEveryShardGetsItsRepliesInOrder() throws Exception {
        RespClient a = client();
        List<String[]> commands = new ArrayList<>();
        for (int i = 0; i < 200; i++) commands.add(new String[] {"SET", "k" + i, "v" + i});
        for (int i = 0; i < 200; i++) commands.add(new String[] {"GET", "k" + i});
        a.send(pipeline(commands));
        for (int i = 0; i < 200; i++) assertEquals(OK, a.reply());
        for (int i = 0; i < 200; i++) assertEquals(bulk("v" + i), a.reply(), "GET k" + i);
    }

    @Test
    void oneClientsCommandsTakeEffectInTheOrderItSentThem() throws Exception {
        RespClient a = client();
        String x = keyOn(1, "x");
        String y = keyOn(2, "y");
        a.send(pipeline(List.of(new String[] {"SET", x, "1"}, new String[] {"INCR", x}, new String[] {"SET", y, "a"},
                new String[] {"GET", x}, new String[] {"PING"}, new String[] {"GET", y})));
        assertEquals(OK, a.reply());
        assertEquals(integer(2), a.reply());
        assertEquals(OK, a.reply());
        assertEquals(bulk("2"), a.reply());
        assertEquals("+PONG\r\n", a.reply());
        assertEquals(bulk("a"), a.reply());
    }

    @Test
    void everyClientSeesTheSameKeys() throws Exception {
        List<RespClient> clients = new ArrayList<>();
        for (int i = 0; i < SHARDS; i++) clients.add(client());
        for (int shard = 0; shard < SHARDS; shard++) {
            clients.get(shard).call("SET", keyOn(shard, "own"), "from " + shard);
        }
        for (RespClient c : clients) {
            for (int shard = 0; shard < SHARDS; shard++) {
                assertEquals(bulk("from " + shard), c.call("GET", keyOn(shard, "own")));
            }
        }
    }

    @Test
    void blpopIsServedByAPushFromAnyClient() throws Exception {
        for (int shard = 0; shard < SHARDS; shard++) {
            String list = keyOn(shard, "list");
            RespClient waiter = client();
            RespClient pusher = client();
            waiter.send(pipeline(List.of(new String[] {"BLPOP", list, "0"}, new String[] {"PING"})));
            assertTrue(waiter.silentFor(Duration.ofMillis(100)), "BLPOP answered before the push, shard " + shard);
            assertEquals(integer(1), pusher.call("RPUSH", list, "x"));
            assertEquals(array(bulk(list), bulk("x")), waiter.reply());
            assertEquals("+PONG\r\n", waiter.reply());
        }
    }

    @Test
    void blpopTimesOutOnEveryShard() throws Exception {
        RespClient a = client();
        for (int shard = 0; shard < SHARDS; shard++) {
            a.send(pipeline(List.of(new String[] {"BLPOP", keyOn(shard, "none"), "0.05"}, new String[] {"PING"})));
            assertEquals(NIL_ARRAY, a.reply());
            assertEquals("+PONG\r\n", a.reply());
        }
    }

    @Test
    void xreadBlockIsWokenByAnXaddFromAnyClient() throws Exception {
        String stream = keyOn(3, "stream");
        RespClient reader = client();
        RespClient writer = client();
        reader.send(RespClient.encode("XREAD", "BLOCK", "0", "STREAMS", stream, "$"));
        assertTrue(reader.silentFor(Duration.ofMillis(100)));
        assertEquals(bulk("1-1"), writer.call("XADD", stream, "1-1", "f", "v"));
        assertEquals(array(array(bulk(stream), array(array(bulk("1-1"), array(bulk("f"), bulk("v")))))),
                reader.reply());
    }

    @Test
    void aTransactionWhoseKeysShareATagRunsOnTheirShard() throws Exception {
        RespClient a = client();
        assertEquals(OK, a.call("MULTI"));
        assertEquals(QUEUED, a.call("SET", "{t}:a", "1"));
        assertEquals(QUEUED, a.call("INCR", "{t}:a"));
        assertEquals(QUEUED, a.call("SET", "{t}:b", "x"));
        assertEquals(QUEUED, a.call("PING"));
        assertEquals(array(OK, integer(2), OK, "+PONG\r\n"), a.call("EXEC"));
        assertEquals(bulk("2"), client().call("GET", "{t}:a"));
    }

    @Test
    void concurrentIncrementsFromManyClientsAddUp() throws Exception {
        int clients = 8;
        int increments = 2_000;
        List<String> counters = new ArrayList<>();
        for (int shard = 0; shard < SHARDS; shard++) counters.add(keyOn(shard, "counter"));
        List<RespClient> connections = new ArrayList<>();
        for (int i = 0; i < clients; i++) connections.add(client());
        ExecutorService pool = Executors.newFixedThreadPool(clients);
        try {
            List<Future<?>> done = new ArrayList<>();
            for (RespClient c : connections) {
                done.add(pool.submit(() -> {
                    List<String[]> commands = new ArrayList<>();
                    for (int i = 0; i < increments; i++) commands.add(new String[] {"INCR", counters.get(i % SHARDS)});
                    c.send(pipeline(commands));
                    for (int i = 0; i < increments; i++) c.reply();
                    return null;
                }));
            }
            for (Future<?> f : done) f.get();
        } finally {
            pool.shutdown();
        }
        for (String counter : counters) {
            assertEquals(bulk(String.valueOf(clients * increments / SHARDS)), client().call("GET", counter));
        }
    }

    @Test
    void aTransactionAcrossShardsRunsAsOne() throws Exception {
        RespClient a = client();
        String x = keyOn(1, "x");
        String y = keyOn(2, "y");
        a.call("MULTI");
        a.call("SET", x, "1");
        a.call("SET", y, "2");
        a.call("INCR", x);
        a.call("GET", y);
        assertEquals(array(OK, OK, integer(2), bulk("2")), a.call("EXEC"));
        assertEquals(bulk("2"), client().call("GET", x));
    }

    @Test
    void nobodySeesHalfOfATransactionAcrossShards() throws Exception {
        String a = keyOn(0, "a");
        String b = keyOn(3, "b");
        RespClient writer = client();
        writer.call("SET", a, "0");
        writer.call("SET", b, "0");
        List<RespClient> readers = new ArrayList<>();
        for (int i = 0; i < 3; i++) readers.add(client());
        ExecutorService pool = Executors.newFixedThreadPool(readers.size() + 1);
        try {
            Future<?> writes = pool.submit(() -> {
                for (int i = 0; i < 300; i++) {
                    writer.send(pipeline(List.of(new String[] {"MULTI"}, new String[] {"INCR", a},
                            new String[] {"INCR", b}, new String[] {"EXEC"})));
                    for (int r = 0; r < 4; r++) writer.reply();
                }
                return null;
            });
            List<Future<?>> reads = new ArrayList<>();
            for (RespClient reader : readers) {
                reads.add(pool.submit(() -> {
                    for (int i = 0; i < 300; i++) {
                        reader.send(pipeline(List.of(new String[] {"MULTI"}, new String[] {"GET", a},
                                new String[] {"GET", b}, new String[] {"EXEC"})));
                        for (int r = 0; r < 3; r++) reader.reply();
                        String both = reader.reply();
                        String[] lines = both.split("\r\n");
                        assertEquals(lines[2], lines[4], "a and b differ: " + both.replace("\r\n", " "));
                    }
                    return null;
                }));
            }
            writes.get();
            for (Future<?> f : reads) f.get();
        } finally {
            pool.shutdown();
        }
        assertEquals(bulk("300"), client().call("GET", b));
    }

    @Test
    void xreadReadsStreamsOnDifferentShards() throws Exception {
        RespClient a = client();
        String s1 = keyOn(0, "s");
        String s2 = keyOn(1, "s");
        a.call("XADD", s1, "1-1", "f", "1");
        a.call("XADD", s2, "2-1", "g", "2");
        assertEquals(array(array(bulk(s1), array(array(bulk("1-1"), array(bulk("f"), bulk("1"))))),
                        array(bulk(s2), array(array(bulk("2-1"), array(bulk("g"), bulk("2")))))),
                a.call("XREAD", "STREAMS", s1, s2, "0-0", "0-0"));
    }

    @Test
    void xreadBlockOverStreamsOnDifferentShardsSaysToUseAHashTag() throws Exception {
        String reply = client().call("XREAD", "BLOCK", "0", "STREAMS", keyOn(0, "s"), keyOn(1, "s"), "$", "$");
        assertTrue(reply.startsWith("-ERR XREAD BLOCK needs all its streams on one shard"), reply);
    }
}
