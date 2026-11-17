package redisclone;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Executes commands against the {@link Store} and (optionally) logs them to the {@link Aof}.
 *
 * CONCURRENCY MODEL: many client threads parse requests in parallel, but a single lock makes
 * command EXECUTION one-at-a-time, like Redis's single-threaded core. Consequences:
 *   - INCR, list ops, etc. are atomic for free.
 *   - The order commands are logged to the AOF equals the order they ran, so replay is exact.
 * Replies are written into an in-memory buffer under the lock; the slow socket write happens
 * OUTSIDE the lock (see ClientHandler), so a slow client cannot stall everyone else.
 *
 * Commands: PING ECHO SET(EX/PX/PXAT) GET DEL EXISTS INCR DECR INCRBY DECRBY
 *           EXPIRE PEXPIRE PEXPIREAT TTL PTTL PERSIST
 *           LPUSH RPUSH LPOP RPOP LLEN LRANGE
 *           DBSIZE FLUSHALL REWRITEAOF COMMAND CONFIG QUIT
 */
public final class CommandHandler {

    private static final String WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value";
    private static final String NOT_INT = "ERR value is not an integer or out of range";

    private final Store store;
    private final ReentrantLock lock = new ReentrantLock();
    private Aof aof; // null = persistence disabled. Only touched while holding the lock.

    public CommandHandler(Store store) {
        this.store = store;
        store.setExpireListener(this::onExpired);
    }

    public void attachAof(Aof aof) {
        lock.lock();
        try {
            this.aof = aof;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Runs one command and writes the reply into {@code reply}.
     *
     * @return true if the connection should be closed afterwards (QUIT)
     */
    public boolean execute(List<byte[]> args, OutputStream reply) throws IOException {
        lock.lock();
        try {
            return dispatch(args, reply);
        } finally {
            lock.unlock();
        }
    }

    /** Called by a background timer: removes expired keys that nobody is reading. */
    public void activeExpireTick() {
        lock.lock();
        try {
            store.activeExpireCycle(1000);
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ dispatch

    private boolean dispatch(List<byte[]> args, OutputStream reply) throws IOException {
        String name = text(args.get(0)).toUpperCase(Locale.ROOT);
        switch (name) {
            case "PING" -> ping(args, reply);
            case "ECHO" -> {
                if (arity(args, reply, 2, 2)) {
                    RespWriter.bulkString(reply, args.get(1));
                }
            }
            case "SET" -> set(args, reply);
            case "GET" -> get(args, reply);
            case "DEL" -> del(args, reply);
            case "EXISTS" -> exists(args, reply);
            case "INCR" -> {
                if (arity(args, reply, 2, 2)) {
                    applyDelta(args, reply, 1);
                }
            }
            case "DECR" -> {
                if (arity(args, reply, 2, 2)) {
                    applyDelta(args, reply, -1);
                }
            }
            case "INCRBY" -> incrByArg(args, reply, false);
            case "DECRBY" -> incrByArg(args, reply, true);
            case "EXPIRE" -> expire(args, reply, true, false);
            case "PEXPIRE" -> expire(args, reply, false, false);
            case "PEXPIREAT" -> expire(args, reply, false, true);
            case "TTL" -> ttl(args, reply, true);
            case "PTTL" -> ttl(args, reply, false);
            case "PERSIST" -> persist(args, reply);
            case "LPUSH" -> push(args, reply, true);
            case "RPUSH" -> push(args, reply, false);
            case "LPOP" -> pop(args, reply, true);
            case "RPOP" -> pop(args, reply, false);
            case "LLEN" -> llen(args, reply);
            case "LRANGE" -> lrange(args, reply);
            case "DBSIZE" -> {
                if (arity(args, reply, 1, 1)) {
                    RespWriter.integer(reply, store.size());
                }
            }
            case "FLUSHALL" -> {
                if (arity(args, reply, 1, 1)) {
                    store.clear();
                    if (log(args, reply)) {
                        RespWriter.simpleString(reply, "OK");
                    }
                }
            }
            case "REWRITEAOF" -> rewriteAof(reply);
            case "COMMAND" -> RespWriter.arrayHeader(reply, 0); // redis-cli asks for docs at startup
            case "CONFIG" -> {
                if (args.size() >= 2 && text(args.get(1)).equalsIgnoreCase("GET")) {
                    RespWriter.arrayHeader(reply, 0); // tools like redis-benchmark ask; we have nothing to report
                } else {
                    RespWriter.error(reply, "ERR unsupported CONFIG subcommand");
                }
            }
            case "QUIT" -> {
                RespWriter.simpleString(reply, "OK");
                return true;
            }
            default -> RespWriter.error(reply, "ERR unknown command '" + name.toLowerCase(Locale.ROOT) + "'");
        }
        return false;
    }

    // ------------------------------------------------------------------ strings

    private void ping(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 1, 2)) {
            return;
        }
        if (args.size() == 1) {
            RespWriter.simpleString(reply, "PONG");
        } else {
            RespWriter.bulkString(reply, args.get(1));
        }
    }

    /** SET key value [EX seconds | PX millis | PXAT epoch-millis] */
    private void set(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 3, -1)) {
            return;
        }
        String key = text(args.get(1));
        byte[] value = args.get(2);
        long expireAt = -1;

        int i = 3;
        while (i < args.size()) {
            String option = text(args.get(i)).toUpperCase(Locale.ROOT);
            boolean knownOption = option.equals("EX") || option.equals("PX") || option.equals("PXAT");
            if (!knownOption || expireAt != -1 || i + 1 >= args.size()) {
                RespWriter.error(reply, "ERR syntax error");
                return;
            }
            Long n = parseLong(args.get(i + 1));
            if (n == null) {
                RespWriter.error(reply, NOT_INT);
                return;
            }
            if (n <= 0) {
                RespWriter.error(reply, "ERR invalid expire time in 'set' command");
                return;
            }
            try {
                expireAt = switch (option) {
                    case "EX" -> Math.addExact(store.now(), Math.multiplyExact(n, 1000L));
                    case "PX" -> Math.addExact(store.now(), n);
                    default -> n; // PXAT: already absolute
                };
            } catch (ArithmeticException e) {
                RespWriter.error(reply, "ERR invalid expire time in 'set' command");
                return;
            }
            i += 2;
        }

        store.set(key, value); // also clears any previous TTL
        if (expireAt != -1) {
            store.setExpiry(key, expireAt);
        }
        // Log the deterministic form: relative expiry becomes an absolute deadline.
        List<byte[]> logged = expireAt == -1
                ? args
                : List.of(bytes("SET"), args.get(1), value, bytes("PXAT"), bytes(Long.toString(expireAt)));
        if (log(logged, reply)) {
            RespWriter.simpleString(reply, "OK");
        }
    }

    private void get(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 2, 2)) {
            return;
        }
        Object value = store.get(text(args.get(1)));
        if (value == null) {
            RespWriter.nullBulkString(reply);
        } else if (value instanceof byte[] bytes) {
            RespWriter.bulkString(reply, bytes);
        } else {
            RespWriter.error(reply, WRONGTYPE);
        }
    }

    private void del(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 2, -1)) {
            return;
        }
        long removed = 0;
        for (int i = 1; i < args.size(); i++) {
            if (store.delete(text(args.get(i)))) {
                removed++;
            }
        }
        if (removed > 0 && !log(args, reply)) {
            return;
        }
        RespWriter.integer(reply, removed);
    }

    private void exists(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 2, -1)) {
            return;
        }
        long count = 0;
        for (int i = 1; i < args.size(); i++) {
            if (store.exists(text(args.get(i)))) {
                count++;
            }
        }
        RespWriter.integer(reply, count);
    }

    // ------------------------------------------------------------------ counters

    private void incrByArg(List<byte[]> args, OutputStream reply, boolean negate) throws IOException {
        if (!arity(args, reply, 3, 3)) {
            return;
        }
        Long n = parseLong(args.get(2));
        if (n == null) {
            RespWriter.error(reply, NOT_INT);
            return;
        }
        try {
            applyDelta(args, reply, negate ? Math.negateExact(n) : n);
        } catch (ArithmeticException e) {
            RespWriter.error(reply, "ERR decrement would overflow");
        }
    }

    /** Shared by INCR, DECR, INCRBY, DECRBY. Atomic because we hold the global lock. */
    private void applyDelta(List<byte[]> args, OutputStream reply, long delta) throws IOException {
        String key = text(args.get(1));
        Object current = store.get(key);
        long base = 0; // a missing key counts as 0
        if (current instanceof byte[] bytes) {
            Long n = parseLong(bytes);
            if (n == null) {
                RespWriter.error(reply, NOT_INT);
                return;
            }
            base = n;
        } else if (current != null) {
            RespWriter.error(reply, WRONGTYPE);
            return;
        }
        long result;
        try {
            result = Math.addExact(base, delta);
        } catch (ArithmeticException e) {
            RespWriter.error(reply, "ERR increment or decrement would overflow");
            return;
        }
        byte[] newValue = Long.toString(result).getBytes(StandardCharsets.US_ASCII);
        if (current == null) {
            store.set(key, newValue);
        } else {
            store.update(key, newValue); // INCR keeps the key's TTL
        }
        if (log(args, reply)) { // INCR is deterministic given the same prior state, so log as-is
            RespWriter.integer(reply, result);
        }
    }

    // ------------------------------------------------------------------ expiry

    /** EXPIRE (seconds, relative), PEXPIRE (millis, relative), PEXPIREAT (millis, absolute). */
    private void expire(List<byte[]> args, OutputStream reply, boolean seconds, boolean absolute)
            throws IOException {
        if (!arity(args, reply, 3, 3)) {
            return;
        }
        Long n = parseLong(args.get(2));
        if (n == null) {
            RespWriter.error(reply, NOT_INT);
            return;
        }
        String key = text(args.get(1));
        if (!store.exists(key)) {
            RespWriter.integer(reply, 0);
            return;
        }
        long at;
        try {
            at = absolute ? n : Math.addExact(store.now(), seconds ? Math.multiplyExact(n, 1000L) : n);
        } catch (ArithmeticException e) {
            RespWriter.error(reply, "ERR invalid expire time in '" + text(args.get(0)).toLowerCase(Locale.ROOT) + "' command");
            return;
        }
        // A deadline already in the past deletes the key right away. NOT while replaying the AOF,
        // where every key must stay alive until the log has been fully applied.
        if (at <= store.now() && !store.isLoading()) {
            store.delete(key);
            if (!log(List.of(bytes("DEL"), args.get(1)), reply)) {
                return;
            }
        } else {
            store.setExpiry(key, at);
            if (!log(List.of(bytes("PEXPIREAT"), args.get(1), bytes(Long.toString(at))), reply)) {
                return;
            }
        }
        RespWriter.integer(reply, 1);
    }

    private void ttl(List<byte[]> args, OutputStream reply, boolean seconds) throws IOException {
        if (!arity(args, reply, 2, 2)) {
            return;
        }
        String key = text(args.get(1));
        if (!store.exists(key)) {
            RespWriter.integer(reply, -2); // no such key
            return;
        }
        long at = store.getExpiry(key);
        if (at == -1) {
            RespWriter.integer(reply, -1); // key exists, no TTL
            return;
        }
        long remainingMs = Math.max(0, at - store.now());
        RespWriter.integer(reply, seconds ? (remainingMs + 500) / 1000 : remainingMs);
    }

    private void persist(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 2, 2)) {
            return;
        }
        String key = text(args.get(1));
        boolean removed = store.exists(key) && store.removeExpiry(key);
        if (removed && !log(args, reply)) {
            return;
        }
        RespWriter.integer(reply, removed ? 1 : 0);
    }

    // ------------------------------------------------------------------ lists

    private void push(List<byte[]> args, OutputStream reply, boolean left) throws IOException {
        if (!arity(args, reply, 3, -1)) {
            return;
        }
        String key = text(args.get(1));
        Object current = store.get(key);
        Store.ListValue list;
        if (current == null) {
            list = new Store.ListValue();
        } else if (current instanceof Store.ListValue existing) {
            list = existing;
        } else {
            RespWriter.error(reply, WRONGTYPE);
            return;
        }
        for (int i = 2; i < args.size(); i++) {
            if (left) {
                list.addFirst(args.get(i)); // LPUSH l a b c  ->  c b a
            } else {
                list.addLast(args.get(i));
            }
        }
        if (current == null) {
            store.set(key, list);
        }
        if (log(args, reply)) {
            RespWriter.integer(reply, list.size());
        }
    }

    private void pop(List<byte[]> args, OutputStream reply, boolean left) throws IOException {
        if (!arity(args, reply, 2, 2)) {
            return;
        }
        String key = text(args.get(1));
        Object current = store.get(key);
        if (current == null) {
            RespWriter.nullBulkString(reply);
            return;
        }
        if (!(current instanceof Store.ListValue list)) {
            RespWriter.error(reply, WRONGTYPE);
            return;
        }
        byte[] element = left ? list.pollFirst() : list.pollLast();
        if (list.isEmpty()) {
            store.delete(key); // Redis never keeps empty lists around
        }
        if (log(args, reply)) {
            RespWriter.bulkString(reply, element);
        }
    }

    private void llen(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 2, 2)) {
            return;
        }
        Object current = store.get(text(args.get(1)));
        if (current == null) {
            RespWriter.integer(reply, 0);
        } else if (current instanceof Store.ListValue list) {
            RespWriter.integer(reply, list.size());
        } else {
            RespWriter.error(reply, WRONGTYPE);
        }
    }

    /** LRANGE key start stop: inclusive indexes; negative counts from the end (-1 = last). */
    private void lrange(List<byte[]> args, OutputStream reply) throws IOException {
        if (!arity(args, reply, 4, 4)) {
            return;
        }
        Long start = parseLong(args.get(2));
        Long stop = parseLong(args.get(3));
        if (start == null || stop == null) {
            RespWriter.error(reply, NOT_INT);
            return;
        }
        Object current = store.get(text(args.get(1)));
        if (current == null) {
            RespWriter.arrayHeader(reply, 0);
            return;
        }
        if (!(current instanceof Store.ListValue list)) {
            RespWriter.error(reply, WRONGTYPE);
            return;
        }
        long len = list.size();
        long from = start < 0 ? start + len : start;
        long to = stop < 0 ? stop + len : stop;
        if (from < 0) {
            from = 0;
        }
        if (from > to || from >= len) {
            RespWriter.arrayHeader(reply, 0);
            return;
        }
        if (to >= len) {
            to = len - 1;
        }
        byte[][] elements = list.toArray(new byte[0][]);
        RespWriter.arrayHeader(reply, (int) (to - from + 1));
        for (long i = from; i <= to; i++) {
            RespWriter.bulkString(reply, elements[(int) i]);
        }
    }

    // ------------------------------------------------------------------ persistence

    private void rewriteAof(OutputStream reply) throws IOException {
        if (aof == null) {
            RespWriter.error(reply, "ERR AOF is not enabled (start with --appendonly yes)");
            return;
        }
        try {
            aof.rewrite(store);
            RespWriter.simpleString(reply, "OK");
        } catch (IOException e) {
            RespWriter.error(reply, "ERR AOF rewrite failed: " + e.getMessage());
        }
    }

    /**
     * Appends a command to the AOF (no-op if persistence is off).
     * Called AFTER the in-memory change, BEFORE replying: the client only sees success once logged.
     *
     * @return false if logging failed (an error reply has already been written)
     */
    private boolean log(List<byte[]> command, OutputStream reply) throws IOException {
        if (aof == null) {
            return true;
        }
        try {
            aof.append(command);
            return true;
        } catch (IOException e) {
            RespWriter.error(reply, "MISCONF Errors writing to the AOF file: " + e.getMessage());
            return false;
        }
    }

    /** Every expiry (lazy or active) is logged as DEL, so replay doesn't depend on wall-clock time. */
    private void onExpired(String key) {
        if (aof == null) {
            return;
        }
        try {
            aof.append(List.of(bytes("DEL"), key.getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            System.err.println("AOF write failed while logging expiry of '" + key + "': " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ helpers

    /** @param max -1 for "no upper limit" */
    private static boolean arity(List<byte[]> args, OutputStream reply, int min, int max) throws IOException {
        if (args.size() < min || (max != -1 && args.size() > max)) {
            RespWriter.error(reply, "ERR wrong number of arguments for '"
                    + text(args.get(0)).toLowerCase(Locale.ROOT) + "' command");
            return false;
        }
        return true;
    }

    /** Strict integer parse: digits with an optional leading '-', otherwise null. */
    private static Long parseLong(byte[] raw) {
        if (raw.length == 0 || raw.length > 20) {
            return null;
        }
        String s = new String(raw, StandardCharsets.US_ASCII);
        char first = s.charAt(0);
        if (first != '-' && (first < '0' || first > '9')) {
            return null; // rejects "+5", " 5", etc.
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
