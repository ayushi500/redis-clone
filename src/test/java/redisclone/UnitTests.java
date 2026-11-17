package redisclone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dependency-free unit tests. Run:  java -cp out redisclone.UnitTests
 *
 * Time is faked (an AtomicLong), so expiry tests run instantly and deterministically.
 * Replies are shown compactly: CRLF becomes '|', e.g.  "+OK|"  ":3|"  "$1|v|"  "$-1|" (null).
 */
public final class UnitTests {

    private static int passed;
    private static int failed;

    /** One isolated server engine with a controllable clock. */
    private static final class Env {
        final AtomicLong clock = new AtomicLong(1000);
        final Store store = new Store(clock::get);
        final CommandHandler handler = new CommandHandler(store);
    }

    private static void check(String name, Object expected, Object actual) {
        if (Objects.equals(expected, actual)) {
            passed++;
            System.out.println("  ok    " + name);
        } else {
            failed++;
            System.out.println("  FAIL  " + name + ": expected <" + expected + "> but got <" + actual + ">");
        }
    }

    private static String run(Env e, String... args) throws IOException {
        List<byte[]> command = new ArrayList<>();
        for (String a : args) {
            command.add(a.getBytes(StandardCharsets.UTF_8));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        e.handler.execute(command, out);
        return out.toString(StandardCharsets.UTF_8).replace("\r\n", "|");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("strings and counters");
        stringsAndCounters();
        System.out.println("expiry");
        expiry();
        System.out.println("lists");
        lists();
        System.out.println("AOF persistence");
        persistence();
        System.out.println("\n" + passed + " passed, " + failed + " failed");
        System.exit(failed == 0 ? 0 : 1);
    }

    private static void stringsAndCounters() throws IOException {
        Env e = new Env();
        check("SET", "+OK|", run(e, "SET", "k", "v"));
        check("GET", "$1|v|", run(e, "GET", "k"));
        check("GET missing", "$-1|", run(e, "GET", "nope"));
        check("INCR creates key at 1", ":1|", run(e, "INCR", "n"));
        check("INCR again", ":2|", run(e, "INCR", "n"));
        check("DECR", ":1|", run(e, "DECR", "n"));
        check("INCRBY", ":11|", run(e, "INCRBY", "n", "10"));
        check("DECRBY", ":8|", run(e, "DECRBY", "n", "3"));
        check("GET after INCR is a string", "$1|8|", run(e, "GET", "n"));
        run(e, "SET", "s", "abc");
        check("INCR on non-number", "-ERR value is not an integer or out of range|", run(e, "INCR", "s"));
        check("INCRBY non-number arg", "-ERR value is not an integer or out of range|", run(e, "INCRBY", "n", "x"));
        run(e, "SET", "max", String.valueOf(Long.MAX_VALUE));
        check("INCR overflow", "-ERR increment or decrement would overflow|", run(e, "INCR", "max"));
        check("EXISTS counts keys", ":2|", run(e, "EXISTS", "k", "n", "nope"));
        check("DEL returns removed count", ":2|", run(e, "DEL", "k", "n", "nope"));
        check("DBSIZE", ":2|", run(e, "DBSIZE"));
        check("wrong arity", "-ERR wrong number of arguments for 'get' command|", run(e, "GET"));
        check("unknown command", "-ERR unknown command 'fly'|", run(e, "FLY"));
    }

    private static void expiry() throws IOException {
        Env e = new Env(); // clock = 1000
        run(e, "SET", "k", "v", "EX", "10"); // deadline = 11000
        check("TTL right after SET", ":10|", run(e, "TTL", "k"));
        e.clock.set(5000);
        check("TTL after 4s", ":6|", run(e, "TTL", "k"));
        check("PTTL in millis", ":6000|", run(e, "PTTL", "k"));
        e.clock.set(11000);
        check("key gone at deadline (lazy)", "$-1|", run(e, "GET", "k"));
        check("TTL of missing key is -2", ":-2|", run(e, "TTL", "k"));

        e = new Env();
        run(e, "SET", "k", "v", "EX", "10");
        run(e, "SET", "k", "w");
        check("plain SET clears the TTL", ":-1|", run(e, "TTL", "k"));

        run(e, "SET", "n", "1", "EX", "10");
        run(e, "INCR", "n");
        check("INCR keeps the TTL", ":10|", run(e, "TTL", "n"));
        check("PERSIST removes TTL", ":1|", run(e, "PERSIST", "n"));
        check("TTL after PERSIST", ":-1|", run(e, "TTL", "n"));
        check("PERSIST on key without TTL", ":0|", run(e, "PERSIST", "n"));

        run(e, "SET", "x", "1");
        check("EXPIRE existing key", ":1|", run(e, "EXPIRE", "x", "5"));
        check("TTL after EXPIRE", ":5|", run(e, "TTL", "x"));
        check("EXPIRE missing key", ":0|", run(e, "EXPIRE", "nokey", "5"));
        check("EXPIRE with 0 deletes now", ":1|", run(e, "EXPIRE", "x", "0"));
        check("deleted by EXPIRE 0", ":0|", run(e, "EXISTS", "x"));

        e = new Env();
        run(e, "SET", "a", "1", "PX", "100");
        run(e, "SET", "b", "2");
        e.clock.addAndGet(200);
        check("overdue key still counted before cleanup", ":2|", run(e, "DBSIZE"));
        check("active cycle removes exactly the overdue key", 1, e.store.activeExpireCycle(100));
        check("DBSIZE after active expiry", ":1|", run(e, "DBSIZE"));

        e = new Env();
        run(e, "SET", "k", "v", "PX", "100");
        run(e, "SET", "k", "v2", "PX", "5000"); // re-set: the first heap entry is now stale
        e.clock.addAndGet(200);
        check("stale heap entry does not delete re-set key", 0, e.store.activeExpireCycle(100));
        check("key still alive", "$2|v2|", run(e, "GET", "k"));

        e = new Env();
        check("EX 0 rejected", "-ERR invalid expire time in 'set' command|", run(e, "SET", "k", "v", "EX", "0"));
        check("EX non-number rejected", "-ERR value is not an integer or out of range|", run(e, "SET", "k", "v", "EX", "abc"));
        check("unknown option rejected", "-ERR syntax error|", run(e, "SET", "k", "v", "FOO"));
        check("two expiry options rejected", "-ERR syntax error|", run(e, "SET", "k", "v", "EX", "1", "PX", "5"));
    }

    private static void lists() throws IOException {
        Env e = new Env();
        check("RPUSH", ":3|", run(e, "RPUSH", "l", "a", "b", "c"));
        check("LPUSH puts last arg first", ":5|", run(e, "LPUSH", "l", "y", "z"));
        check("LRANGE all", "*5|$1|z|$1|y|$1|a|$1|b|$1|c|", run(e, "LRANGE", "l", "0", "-1"));
        check("LRANGE negative window", "*2|$1|b|$1|c|", run(e, "LRANGE", "l", "-2", "-1"));
        check("LRANGE out of range", "*0|", run(e, "LRANGE", "l", "10", "20"));
        check("LRANGE stop beyond end is clamped", "*2|$1|b|$1|c|", run(e, "LRANGE", "l", "3", "100"));
        check("LLEN", ":5|", run(e, "LLEN", "l"));
        check("LPOP", "$1|z|", run(e, "LPOP", "l"));
        check("RPOP", "$1|c|", run(e, "RPOP", "l"));
        run(e, "LPOP", "l");
        run(e, "LPOP", "l");
        run(e, "LPOP", "l");
        check("empty list is removed", ":0|", run(e, "EXISTS", "l"));
        check("LPOP on missing key", "$-1|", run(e, "LPOP", "l"));
        check("LLEN on missing key", ":0|", run(e, "LLEN", "l"));
        run(e, "SET", "s", "v");
        String wrong = "-WRONGTYPE Operation against a key holding the wrong kind of value|";
        check("LPUSH on string", wrong, run(e, "LPUSH", "s", "x"));
        run(e, "RPUSH", "l2", "a");
        check("GET on list", wrong, run(e, "GET", "l2"));
        check("INCR on list", wrong, run(e, "INCR", "l2"));
    }

    private static void persistence() throws Exception {
        Path dir = Files.createTempDirectory("redisclone-test");

        // 1) write, "crash", recover
        Path file = dir.resolve("a.aof");
        Env e1 = new Env();
        Aof aof = new Aof(file, Aof.Fsync.NO);
        e1.handler.attachAof(aof);
        run(e1, "SET", "name", "ayushi");
        run(e1, "SET", "counter", "1");
        run(e1, "INCR", "counter");
        run(e1, "RPUSH", "l", "a", "b");
        run(e1, "SET", "temp", "x", "EX", "100");
        run(e1, "SET", "gone", "1");
        run(e1, "DEL", "gone");
        run(e1, "FLUSHALL");
        run(e1, "SET", "after", "flush");
        aof.close();
        Env e2 = new Env();
        int replayed = Aof.load(file, e2.store, e2.handler);
        check("replayed all commands", 9, replayed);
        check("recovered: only post-FLUSHALL data", ":1|", run(e2, "DBSIZE"));
        check("recovered value", "$5|flush|", run(e2, "GET", "after"));

        // 2) richer state, including TTL and lists
        file = dir.resolve("b.aof");
        e1 = new Env();
        aof = new Aof(file, Aof.Fsync.NO);
        e1.handler.attachAof(aof);
        run(e1, "SET", "counter", "1");
        run(e1, "INCR", "counter");
        run(e1, "RPUSH", "l", "a", "b", "c");
        run(e1, "LPOP", "l");
        run(e1, "SET", "temp", "x", "EX", "100");
        run(e1, "EXPIRE", "counter", "50");
        aof.close();
        e2 = new Env();
        Aof.load(file, e2.store, e2.handler);
        check("counter recovered", "$1|2|", run(e2, "GET", "counter"));
        check("TTL recovered as absolute deadline", ":50|", run(e2, "TTL", "counter"));
        check("list recovered", "*2|$1|b|$1|c|", run(e2, "LRANGE", "l", "0", "-1"));
        check("SET EX recovered", ":100|", run(e2, "TTL", "temp"));

        // 3) torn tail: crash in the middle of writing a command
        file = dir.resolve("c.aof");
        e1 = new Env();
        aof = new Aof(file, Aof.Fsync.NO);
        e1.handler.attachAof(aof);
        run(e1, "SET", "safe", "1");
        aof.close();
        long goodSize = Files.size(file);
        Files.write(file, "*3\r\n$3\r\nSET\r\n$1\r\nk".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
        e2 = new Env();
        Aof.load(file, e2.store, e2.handler);
        check("complete commands survive a torn tail", "$1|1|", run(e2, "GET", "safe"));
        check("torn command was not applied", "$-1|", run(e2, "GET", "k"));
        check("file truncated back to last good command", goodSize, Files.size(file));

        // 4) corruption in the middle must refuse to load rather than guess
        file = dir.resolve("d.aof");
        Files.write(file, "*1\r\n#bad\r\n".getBytes(StandardCharsets.UTF_8));
        e2 = new Env();
        boolean refused = false;
        try {
            Aof.load(file, e2.store, e2.handler);
        } catch (IOException ex) {
            refused = true;
        }
        check("corrupt AOF is rejected", true, refused);

        // 5) the tricky one: a key expires, then is re-created, then the server restarts
        file = dir.resolve("e.aof");
        e1 = new Env(); // clock = 1000
        aof = new Aof(file, Aof.Fsync.NO);
        e1.handler.attachAof(aof);
        run(e1, "SET", "k", "5", "PX", "1000"); // deadline 2000
        e1.clock.set(1500);
        check("alive before deadline", ":6|", run(e1, "INCR", "k"));
        e1.clock.set(2500);
        check("expired key restarts from 0", ":1|", run(e1, "INCR", "k"));
        aof.close();
        e2 = new Env();
        e2.clock.set(9000); // restart much later
        Aof.load(file, e2.store, e2.handler);
        check("replay gives the same final value", "$1|1|", run(e2, "GET", "k"));
        check("and no stale TTL", ":-1|", run(e2, "TTL", "k"));

        // 6) active expiry also logs a DEL
        file = dir.resolve("f.aof");
        e1 = new Env();
        aof = new Aof(file, Aof.Fsync.NO);
        e1.handler.attachAof(aof);
        run(e1, "SET", "t", "1", "PX", "100");
        e1.clock.addAndGet(500);
        e1.handler.activeExpireTick();
        run(e1, "SET", "t", "fresh");
        aof.close();
        e2 = new Env();
        Aof.load(file, e2.store, e2.handler);
        check("active-expired then re-set key recovers", "$5|fresh|", run(e2, "GET", "t"));
        check("with no TTL", ":-1|", run(e2, "TTL", "t"));

        // 7) rewrite (compaction)
        file = dir.resolve("g.aof");
        e1 = new Env();
        aof = new Aof(file, Aof.Fsync.NO);
        e1.handler.attachAof(aof);
        for (int i = 0; i < 200; i++) {
            run(e1, "SET", "counter", String.valueOf(i));
        }
        run(e1, "SET", "keep", "v", "EX", "100");
        run(e1, "RPUSH", "list", "1", "2", "3");
        run(e1, "SET", "gone", "x");
        run(e1, "DEL", "gone");
        long before = Files.size(file);
        check("REWRITEAOF ok", "+OK|", run(e1, "REWRITEAOF"));
        long after = Files.size(file);
        check("rewrite shrinks the log", true, after < before);
        run(e1, "SET", "after", "1"); // appends must still land in the NEW file
        aof.close();
        e2 = new Env();
        Aof.load(file, e2.store, e2.handler);
        check("rewrite: value", "$3|199|", run(e2, "GET", "counter"));
        check("rewrite: TTL", ":100|", run(e2, "TTL", "keep"));
        check("rewrite: list", "*3|$1|1|$1|2|$1|3|", run(e2, "LRANGE", "list", "0", "-1"));
        check("rewrite: deleted key stays deleted", ":0|", run(e2, "EXISTS", "gone"));
        check("rewrite: later append survived", "$1|1|", run(e2, "GET", "after"));
        check("REWRITEAOF without AOF", "-ERR AOF is not enabled (start with --appendonly yes)|",
                run(new Env(), "REWRITEAOF"));
    }
}
