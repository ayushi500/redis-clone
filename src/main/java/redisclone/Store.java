package redisclone;

import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The in-memory keyspace: key -> value, plus an optional expiry time per key.
 *
 * Values are either {@code byte[]} (a string) or {@link ListValue} (a list).
 *
 * THREAD SAFETY: this class is NOT thread-safe on purpose. Every access happens while
 * holding the single lock in {@link CommandHandler}, which is how real Redis behaves
 * (commands run one at a time). That makes INCR, list ops and AOF ordering simple and correct.
 *
 * EXPIRY uses two cooperating mechanisms, like Redis:
 *  1. LAZY: when a key is touched, check its deadline first; if passed, delete it and act as if absent.
 *  2. ACTIVE: a background tick removes expired keys nobody touches. A min-heap ordered by
 *     deadline makes this cheap: only look at the top while it is due.
 *
 * Whenever a key is expired (by either path) the {@code expireListener} is called so the AOF
 * can record a DEL. Without that, replaying the log later would resurrect wrong state.
 */
public final class Store {

    /** Marker type for list values. */
    public static final class ListValue extends ArrayDeque<byte[]> {
        private static final long serialVersionUID = 1L;
    }

    private record ExpiryEntry(long at, String key) {
    }

    private final Map<String, Object> data = new HashMap<>();
    private final Map<String, Long> expires = new HashMap<>();   // key -> absolute deadline (epoch ms)
    private final PriorityQueue<ExpiryEntry> expiryQueue =
            new PriorityQueue<>(Comparator.comparingLong(ExpiryEntry::at));
    private final LongSupplier clock;
    private boolean loading;                                      // true while replaying the AOF
    private Consumer<String> expireListener = key -> { };

    public Store() {
        this(System::currentTimeMillis);
    }

    /** The clock is injectable so tests can move time without sleeping. */
    public Store(LongSupplier clock) {
        this.clock = clock;
    }

    public long now() {
        return clock.getAsLong();
    }

    /**
     * While loading the AOF, expiry is switched OFF: every key is treated as alive, exactly as it
     * was when the original command ran. Keys that are overdue get cleaned up after loading.
     */
    public void setLoading(boolean loading) {
        this.loading = loading;
    }

    public boolean isLoading() {
        return loading;
    }

    public void setExpireListener(Consumer<String> listener) {
        this.expireListener = listener;
    }

    // ---- reads (all apply lazy expiry first) ----

    public Object get(String key) {
        expireIfNeeded(key);
        return data.get(key);
    }

    public boolean exists(String key) {
        expireIfNeeded(key);
        return data.containsKey(key);
    }

    // ---- writes ----

    /** Stores a value and CLEARS any TTL (this is what SET does). */
    public void set(String key, Object value) {
        data.put(key, value);
        expires.remove(key);
    }

    /** Replaces a value but KEEPS the TTL (what INCR does). */
    public void update(String key, Object value) {
        data.put(key, value);
    }

    public boolean delete(String key) {
        expireIfNeeded(key);
        return removeInternal(key);
    }

    public void clear() {
        data.clear();
        expires.clear();
        expiryQueue.clear();
    }

    // ---- expiry ----

    public void setExpiry(String key, long deadlineMillis) {
        expires.put(key, deadlineMillis);
        expiryQueue.add(new ExpiryEntry(deadlineMillis, key));
    }

    /** @return true if the key had a TTL that was removed. */
    public boolean removeExpiry(String key) {
        return expires.remove(key) != null;
    }

    /** @return the deadline in epoch millis, or -1 if the key has no TTL. */
    public long getExpiry(String key) {
        Long at = expires.get(key);
        return at == null ? -1 : at;
    }

    /**
     * Background cleanup. Pops due entries off the heap. An entry is STALE if the key was deleted
     * or given a different deadline since the entry was pushed; those are skipped (lazy deletion
     * from the heap, because removing from the middle of a heap is O(n)).
     *
     * @param maxWork upper bound on heap entries examined, so one tick can't hog the lock
     * @return how many keys were really expired
     */
    public int activeExpireCycle(int maxWork) {
        if (loading) {
            return 0;
        }
        long now = now();
        int removed = 0;
        for (int work = 0; work < maxWork; work++) {
            ExpiryEntry top = expiryQueue.peek();
            if (top == null || top.at() > now) {
                break;
            }
            expiryQueue.poll();
            Long current = expires.get(top.key());
            if (current != null && current == top.at()) {
                removeInternal(top.key());
                expireListener.accept(top.key());
                removed++;
            }
        }
        return removed;
    }

    // ---- misc ----

    /** Number of keys, including overdue ones not cleaned up yet (same as Redis DBSIZE). */
    public int size() {
        return data.size();
    }

    public Set<Map.Entry<String, Object>> entries() {
        return data.entrySet();
    }

    private void expireIfNeeded(String key) {
        if (loading) {
            return;
        }
        Long at = expires.get(key);
        if (at != null && at <= now()) {
            removeInternal(key);
            expireListener.accept(key);
        }
    }

    private boolean removeInternal(String key) {
        expires.remove(key);
        return data.remove(key) != null;
    }
}
