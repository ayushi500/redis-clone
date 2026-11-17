# redis-clone

A Redis-compatible in-memory data store written in Java 17 from scratch: plain sockets, no
frameworks, no dependencies. It speaks the real Redis protocol, so `redis-cli` and Redis client
libraries can talk to it.

## What it does

| Area | Details |
|---|---|
| Protocol | RESP parser (arrays of bulk strings) plus inline commands; handles fragmented and pipelined input |
| Strings | `SET` (with `EX` / `PX` / `PXAT`), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Expiry | `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`; lazy + active (min-heap) expiration |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE` |
| Persistence | Append-only file with 3 fsync policies, crash recovery, torn-write repair, log compaction (`REWRITEAOF`) |
| Server | `DBSIZE`, `FLUSHALL`, `PING`, `ECHO`, `QUIT`, minimal `COMMAND` / `CONFIG GET` for tool compatibility |

## Run

```bash
mkdir -p out && javac -d out $(find src -name '*.java')

java -cp out redisclone.Main                                   # port 6379, no persistence
java -cp out redisclone.Main --port 6380 --appendonly yes \
     --dir ./data --appendfsync everysec                       # with persistence

redis-cli -p 6380 SET name ayushi EX 60
redis-cli -p 6380 TTL name
```

Options: `--port`, `--appendonly yes|no`, `--dir`, `--appendfilename`, `--appendfsync always|everysec|no`.

## Tests

```bash
./run_tests.sh
```

1. **Unit tests** (`src/test`): dependency-free, with a fake clock so expiry tests run instantly.
2. **Integration tests** (`tests/smoke_test.py`): raw RESP over TCP: framing, pipelining, binary values,
   50 concurrent clients, and an atomicity check (20 clients x 200 `INCR` must equal exactly 4000).
3. **Crash tests** (`tests/persistence_test.py`): the server is killed with `SIGKILL`, restarted, and the
   data must come back, including a torn final write and a log compaction.

## Benchmark

```bash
java -cp out redisclone.Benchmark 6379 50 100000 1 16      # port clients requests pipeline valueBytes
redis-benchmark -p 6379 -c 50 -n 100000 -P 1 -d 16 -t set,get   # the real thing, for comparison
```

Run both on the same machine in the same session and compare ratios, not absolute numbers.
Also compare `--appendfsync always` against `everysec` to see what durability costs.

## Design decisions

- **One lock around command execution.** Clients are parsed on separate threads, but commands run one
  at a time, like Redis's single-threaded core. This makes `INCR` and list operations atomic and
  guarantees the AOF order equals the execution order. Replies are built in memory under the lock and
  written to the socket outside it, so a slow client cannot block others.
- **TCP is a stream.** The parser reads exactly the bytes the length prefixes announce and never
  assumes message boundaries.
- **Expiry is lazy + active.** Touching a key checks its deadline. A background tick pops overdue keys
  from a min-heap. Heap entries are never removed from the middle; stale ones (key deleted or given a
  new deadline) are skipped when they reach the top.
- **The AOF stores deterministic commands.** `SET k v EX 10` is logged as `SET k v PXAT <absolute ms>`,
  because "10 seconds from when?" has no meaning at replay time.
- **Expirations are logged as `DEL`, and expiry is switched off while replaying.** Otherwise a key
  that expired and was later re-created could be replayed into the wrong state. (Covered by a test.)
- **Torn writes are repaired.** A crash can leave half a command at the end of the file. On startup the
  file is truncated back to the last complete command. Corruption in the middle is rejected instead of guessed at.
- **Compaction swaps files atomically.** `REWRITEAOF` writes the minimal command set for the current
  state to a temp file, fsyncs it, then atomically renames it over the log.

## Known limitations

- Thread per connection (cached pool), not an event loop; it will not scale to tens of thousands of clients.
- The global lock means commands cannot use multiple cores.
- Keys are UTF-8 strings (values are binary safe). Only string and list types; no hashes, sets, or sorted sets.
- `REWRITEAOF` blocks all commands while it runs (real Redis forks a child). It is not the standard `BGREWRITEAOF`.
- No RDB-style snapshots, replication, transactions, or pub/sub.
- Heap entries for keys that are re-`SET` many times with a TTL stay until their old deadline passes.
- No authentication or TLS; do not expose it to a network.
- Persistence is tested for process crashes (`kill -9`), not for power loss or a corrupted disk.

## Ideas to extend

Hashes and sets, a `Selector`-based event loop, replication, `MULTI`/`EXEC`, `SET NX/XX`, RDB-style snapshots.
