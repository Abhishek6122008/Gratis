package gratis.jmh;

import gratis.Args;
import gratis.Resp;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Parsing one read's worth of a memtier-style pipeline: 16 commands, 1 SET with a 100-byte value and 15 GETs, as at
 * pipeline 16 with SET:GET 1:10. v1's parser (a verbatim copy) against Resp.parse.
 *
 * <p>Run: {@code bench/jmh.sh ParseBenchmark -prof gc}. The score is ns per 16 commands; {@code gc.alloc.rate.norm} is
 * the bytes allocated per 16 commands.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(3)
@State(Scope.Thread)
public class ParseBenchmark {
    private ByteBuffer buffer;
    private final Args args = new Args();

    private static void command(ByteArrayOutputStream out, String... parts) {
        StringBuilder command = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String part : parts) command.append('$').append(part.length()).append("\r\n").append(part).append("\r\n");
        out.writeBytes(command.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Setup
    public void setUp() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        command(out, "SET", "memtier-4242", "x".repeat(100));
        for (int i = 0; i < 15; i++) command(out, "GET", "memtier-" + (1000 + i * 37));
        buffer = ByteBuffer.wrap(out.toByteArray());
    }

    @Benchmark
    public void v1(Blackhole blackhole) {
        buffer.position(0);
        List<String> command;
        while ((command = V1Parser.parse(buffer)) != null) blackhole.consume(command);
    }

    @Benchmark
    public void current(Blackhole blackhole) throws Resp.ProtocolException {
        buffer.position(0);
        while (Resp.parse(buffer, args)) blackhole.consume(args.size());
    }
}
