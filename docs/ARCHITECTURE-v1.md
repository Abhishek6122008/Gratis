# Gratis v1: how it works

This describes the server at the `v1-baseline` tag: one file, `src/main/java/Main.java`, about 400 lines. Every v2
change is measured against this version, so this file says what it does, where its limits are, and where its time is
likely to go. The "likely bottlenecks" at the end are guesses from reading the code. The Day 3 benchmarks and flame
graphs decide which of them are real.

## In one paragraph

One thread runs a `java.nio` `Selector` loop. It accepts connections, reads whatever bytes have arrived, parses as many
complete RESP commands as the buffer holds, runs each one against plain `HashMap`s, and writes each reply back
immediately with one `write` call. Nothing is shared between threads because there is only one thread. There is no
persistence, no eviction and no memory limit.

## I/O and threading model

- **One thread, one selector.** `main` opens a `ServerSocketChannel` on port 6379 (or `--port N`), registers it for
  `OP_ACCEPT`, and loops on `selector.select(timeout)`.
- **Accept.** Each new client is set non-blocking and registered for `OP_READ`, with its own 64 KB heap `ByteBuffer`
  as the key's attachment. That buffer holds bytes that have been read but not yet parsed.
- **Read.** When a client is readable, the loop calls `read` once into its buffer, then flips it and parses complete
  commands one after another until a command is incomplete. `compact()` keeps the leftover bytes for the next read.
  A `read` of `-1` closes the client and drops its transaction queue.
- **Write.** Each reply is a `String`. `send` turns it into bytes and calls `SocketChannel.write` once. There is no
  write buffer and no `OP_WRITE` interest: the return value of `write` is ignored.
- **Pipelining** works for reading: 16 pipelined commands that arrive in one packet are all parsed in one pass. But
  each reply is written on its own, so 16 commands cost 16 `write` system calls.
- **Timeouts.** Before each `select`, `nextTimeout()` scans every blocked `BLPOP` and `XREAD` client for the nearest
  deadline. After each pass, `expire()` scans them all again and answers the ones whose time is up with a nil.
- **Logging.** Each connect and disconnect prints a line to `System.out`.

## RESP parsing

`parse(ByteBuffer)` returns one command as a `List<String>`, or `null` if the buffer doesn't hold a complete one yet
(and then puts the position back where it started).

- A line starting with `*` is a RESP array of bulk strings: `*N`, then N times `$len` and `len` bytes. Each argument
  is copied into a new `byte[]` and decoded as UTF-8 into a `String`.
- Any other line is an inline command, split on whitespace (`PING\r\n` works from `nc`).
- `line()` looks for `\r\n` one byte at a time from the current position. When a command is split across reads, the
  next read parses it again from the start.

## Keyspace

Four static maps, one per kind of data, plus two for blocked clients:

| Map | Holds |
|---|---|
| `store: Map<String, String>` | string values |
| `expiries: Map<String, Long>` | expiry time (epoch ms) for string keys |
| `lists: Map<String, List<String>>` | lists, as `ArrayList` |
| `streams: Map<String, List<Entry>>` | streams, as `ArrayList` of `Entry(ms, seq, fields)` |
| `blocked: Map<String, Deque<Waiter>>` | clients waiting in `BLPOP`, per key, oldest first |
| `readers: List<Reader>` | clients waiting in `XREAD BLOCK` |
| `transactions: Map<SocketChannel, List<List<String>>>` | each client's queued commands after `MULTI` |

Keys and values are Java `String`s, so values are not binary-safe: bytes that are not valid UTF-8 are replaced when
decoded. The three data maps are separate, so one name can be a string, a list and a stream at the same time. `TYPE`
reports the first it finds, in that order, and no command returns `WRONGTYPE`.

## Commands

| Group | Commands |
|---|---|
| Basics | `PING`, `ECHO`, `TYPE` |
| Strings | `SET key value [EX s \| PX ms]`, `GET`, `INCR` (64-bit, errors on non-integers and overflow) |
| Lists | `RPUSH`, `LPUSH`, `LLEN`, `LPOP [count]`, `LRANGE` (negative indexes), `BLPOP key timeout` (one key) |
| Streams | `XADD` (explicit, `ms-*` and `*` IDs), `XRANGE`, `XREAD [BLOCK ms] STREAMS ... ids` (with `$`) |
| Transactions | `MULTI`, `EXEC`, `DISCARD` |

`handle` upper-cases the command name and switches on it. Inside `MULTI`, every command except `MULTI`, `EXEC` and
`DISCARD` is queued and answered `+QUEUED`. `EXEC` runs the queue in order. A command that throws becomes an `-ERR`
entry and the rest still run. `BLPOP` and `XREAD BLOCK` inside `EXEC` return nil instead of blocking.

When `RPUSH`/`LPUSH` adds to a list that has waiters, `serve` hands items to waiters oldest first. When `XADD` adds to
a stream, `wake` re-runs each waiting reader's query and answers the ones that now have entries.

## Expiry

Lazy only. `get(key)` checks `expiries` and deletes the key if its time has passed. Nothing else deletes expired keys,
so a key that is set with a TTL and never read again stays in memory forever. Times come from
`System.currentTimeMillis()`, so a wall-clock jump moves every deadline. `SET` without `EX`/`PX` clears an earlier
expiry. `INCR` keeps it.

## Eviction and memory

None. There is no `maxmemory` and no eviction policy: the heap grows until the JVM throws `OutOfMemoryError`.

## Persistence

None. No RDB, no AOF. A restart loses everything. The benchmarks therefore run Redis and Valkey with persistence off
too.

## Known limits and bugs at v1

Items 1–3 are confirmed by the workspace probe `devlog/probes/day-01/gratis_robustness.py` (2026-11-13). Items 4 and 5
are from reading the code and have not been triggered yet.

1. **Any command that throws outside `MULTI` kills the server.** `GET` with no key throws
   `IndexOutOfBoundsException`. `main` only catches `IOException`, so the exception ends the selector thread and the
   process. The same goes for `SET k v EX abc`, `LRANGE` with a non-number, and others.
2. **A client resetting its connection kills the server.** `read` throws `IOException: Connection reset by peer`,
   which escapes the loop the same way.
3. **A request bigger than 64 KB is never answered.** The buffer fills, `read` returns 0, the command never completes,
   and the client waits forever.
4. **Partial writes lose data.** When the socket's send buffer is full (a slow reader, or a large `LRANGE`), `write`
   sends part of the reply and the rest is dropped.
5. **Writes to a closed client** can throw `IOException` ("Broken pipe") and, like 2, end the server.

## Likely bottlenecks

To be checked on Day 3 with real numbers and flame graphs:

1. **One `write` system call per reply.** Pipelined commands are read in one go but answered one write at a time.
   This should show most at pipeline 16.
2. **Allocation per command.** Each argument becomes a `byte[]` and then a `String`, each command a `List`, each reply
   a `String` built by concatenation and then another `byte[]` from `getBytes`. At high rates that is GC work on the
   only thread.
3. **One thread.** Everything (parsing, commands, writes, timeouts) runs on one core of eight. Redis has the same
   model, but it does far less work per command.

Smaller costs that matter only in some workloads:

- `LPUSH` inserts at index 0 of an `ArrayList`, and `LPOP`/`BLPOP` remove index 0: each is O(n) in the list's length.
- `XRANGE` and `XREAD` scan the whole stream.
- `nextTimeout()` and `expire()` scan every blocked client on every loop pass.
- `System.out.println` on every connect and disconnect slows down connection storms.
