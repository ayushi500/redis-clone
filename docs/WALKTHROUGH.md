# Walkthrough: how to read and explain this project

## Reading order

1. `RespParser` - how bytes on a socket become a command. Start here; everything depends on it.
2. `RespWriter` - how replies are encoded.
3. `Store` - the data, plus the expiry machinery (lazy + heap).
4. `CommandHandler` - the lock, and one method per command. Read `set`, `applyDelta` (INCR), `expire` and `log` closely.
5. `ClientHandler` / `Main` - the per-connection loop and startup order.
6. `Aof` - persistence: append, replay (`load`), compaction (`rewrite`).
7. `src/test/.../UnitTests.java` - the test cases double as a specification of the tricky behaviour.

## Exercise: prove you understand it

Make each change yourself, then run `./run_tests.sh` and explain which tests fail and why.
- Remove the lock in `CommandHandler.execute`. Which test breaks? (Hint: the atomic INCR integration test.)
- Make `Store.update` clear the TTL. Which unit test fails, and what real-world bug would that be?
- Stop logging `DEL` in `onExpired`. Which test fails, and what state does replay produce?
- Delete the truncate step in `Aof.load`. What happens to the next append after a torn write?
- Add `SET key value NX` and `EXISTS`-style tests for it.

## Questions you should be able to answer

**Why a single lock instead of per-key locks or `ConcurrentHashMap`?**
Redis executes commands serially, and I kept that model. It makes multi-step commands (INCR, list ops)
atomic and makes AOF order equal execution order. The cost is no multi-core command execution; I'd
call that out as a known trade-off, and Redis accepts the same one.

**What happens if a client sends half a command?**
TCP is a byte stream, so reads can split or merge commands. The parser uses the length prefixes to read
exactly the right number of bytes and blocks until they arrive. Tested with fragmented and pipelined input.

**How does expiry work?**
Lazy: check the deadline whenever a key is accessed. Active: a 100 ms background task pops due entries
from a min-heap ordered by deadline, skipping stale entries. Lazy alone would leak memory for keys nobody
reads; active alone would be too slow to be exact.

**Why is the heap cleaned lazily?**
Removing an arbitrary element from a binary heap is O(n). Instead stale entries are detected on pop by
comparing against the key's current deadline in the map.

**Why log `PXAT` instead of `EX`?**
Replay happens at a different time than the original command. A relative TTL would restart on every
recovery. Absolute deadlines make the log deterministic.

**Why log a `DEL` when a key expires, and why disable expiry during replay?**
Consider: `SET k 5 PX 1000`, later the key expires, then `INCR k` creates it fresh with value 1. If
replay ran on a later clock, it would expire `k` earlier than the original did and compute a different
result. Logging the expiry as `DEL` and treating all keys as alive during load reproduces the exact
original sequence. This is how Redis handles it too.

**What does `appendfsync` change?**
`write()` hands bytes to the OS, which survives a process crash. `fsync` forces them to disk, which only
matters for power loss or an OS crash. `always` fsyncs every command (slow, safest); `everysec` does it
in a background thread (lose up to about a second on power loss); `no` leaves it to the OS.

**What if the server crashes in the middle of writing a command?**
The file ends with a partial command. On startup the loader replays everything complete, then truncates
the file to the end of the last complete command, so new appends do not follow garbage.

**How does the log compaction stay crash-safe?**
It writes a complete new file, fsyncs it, then does an atomic rename over the old one. A crash leaves either
the old complete file or the new complete file, never a mix.

**What would you change to scale it?**
Replace thread-per-connection with an NIO `Selector` event loop (one thread handling many sockets), fork
or snapshot for `REWRITEAOF` so it does not block, and add replication. Be honest that the current
version is for learning and does not target production loads.

**What are the limitations?**
See the README: no event loop, strings and lists only, blocking rewrite, tested for process crashes not power loss.
