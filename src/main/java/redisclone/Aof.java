package redisclone;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Append-Only File persistence.
 *
 * Every write command that changes data is appended to a file in the same RESP format clients
 * use. To recover after a crash, replay the file from the start through the normal command
 * handler. Simple, human-readable, and easy to reason about.
 *
 * DURABILITY (the {@link Fsync} policy):
 *   ALWAYS   - fsync after every command. Safest, slowest.
 *   EVERYSEC - fsync in a background thread once a second. Lose at most ~1s on power loss.
 *   NO       - never fsync, leave it to the OS. Fastest, weakest.
 * Note: write() hands bytes to the OS immediately, so a *process* crash (kill -9) loses nothing
 * under any policy. fsync only matters for OS crash or power loss.
 *
 * Commands are logged in a deterministic form: relative times (EX/PX/EXPIRE) are converted to
 * absolute deadlines (PXAT/PEXPIREAT), because "10 seconds from when?" is meaningless on replay.
 */
public final class Aof implements Closeable {

    public enum Fsync {
        ALWAYS, EVERYSEC, NO;

        public static Fsync parse(String s) {
            return switch (s.toLowerCase(Locale.ROOT)) {
                case "always" -> ALWAYS;
                case "everysec" -> EVERYSEC;
                case "no" -> NO;
                default -> throw new IllegalArgumentException("appendfsync must be always, everysec or no");
            };
        }
    }

    private final Path path;
    private final Fsync policy;
    private FileOutputStream out;
    private boolean dirty;
    private ScheduledExecutorService syncer;

    public Aof(Path path, Fsync policy) throws IOException {
        this.path = path;
        this.policy = policy;
        this.out = new FileOutputStream(path.toFile(), true); // true = append
        if (policy == Fsync.EVERYSEC) {
            syncer = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "aof-fsync");
                t.setDaemon(true);
                return t;
            });
            syncer.scheduleWithFixedDelay(this::syncIfDirty, 1, 1, TimeUnit.SECONDS);
        }
    }

    /** Appends one command as a single write() call, so a crash can tear at most the last command. */
    public synchronized void append(List<byte[]> command) throws IOException {
        out.write(RespWriter.encodeCommand(command));
        if (policy == Fsync.ALWAYS) {
            out.getFD().sync();
        } else {
            dirty = true;
        }
    }

    /** Runs on the fsync thread. The slow fsync happens OUTSIDE the lock so appends keep flowing. */
    private void syncIfDirty() {
        FileOutputStream current;
        synchronized (this) {
            if (!dirty) {
                return;
            }
            dirty = false;
            current = out;
        }
        try {
            current.getFD().sync();
        } catch (IOException e) {
            synchronized (this) {
                dirty = true; // try again next second
            }
            System.err.println("AOF fsync failed: " + e.getMessage());
        }
    }

    /**
     * COMPACTION. The log grows forever (SET counter 1, SET counter 2, ...). Rewriting produces
     * the shortest command list that rebuilds the CURRENT state, then atomically swaps files.
     * A crash at any point leaves either the old complete file or the new complete file.
     * (This version blocks other commands while it runs; real Redis forks a child instead.)
     */
    public synchronized void rewrite(Store store) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".rewrite");
        long now = store.now();
        try (FileOutputStream fos = new FileOutputStream(tmp.toFile());
             BufferedOutputStream bos = new BufferedOutputStream(fos, 1 << 16)) {
            for (Map.Entry<String, Object> e : store.entries()) {
                String key = e.getKey();
                long at = store.getExpiry(key);
                if (at != -1 && at <= now) {
                    continue; // already logically expired: don't write it
                }
                byte[] k = key.getBytes(StandardCharsets.UTF_8);
                Object value = e.getValue();
                if (value instanceof byte[] bytes) {
                    bos.write(RespWriter.encodeCommand(List.of(bytes("SET"), k, bytes)));
                } else if (value instanceof Store.ListValue list) {
                    List<byte[]> chunk = new ArrayList<>();
                    chunk.add(bytes("RPUSH"));
                    chunk.add(k);
                    for (byte[] element : list) {
                        chunk.add(element);
                        if (chunk.size() >= 502) { // flush in batches of 500 elements
                            bos.write(RespWriter.encodeCommand(chunk));
                            chunk = new ArrayList<>(List.of(bytes("RPUSH"), k));
                        }
                    }
                    if (chunk.size() > 2) {
                        bos.write(RespWriter.encodeCommand(chunk));
                    }
                }
                if (at != -1) {
                    bos.write(RespWriter.encodeCommand(
                            List.of(bytes("PEXPIREAT"), k, bytes(Long.toString(at)))));
                }
            }
            bos.flush();
            fos.getFD().sync(); // new file must be durable BEFORE it replaces the old one
        }
        out.close();
        try {
            try {
                Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            out = new FileOutputStream(path.toFile(), true); // reopen (new file, or old one if move failed)
            dirty = false;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (syncer != null) {
            syncer.shutdownNow();
        }
        out.getFD().sync();
        out.close();
    }

    /**
     * Replays an AOF file into the store through the normal command handler.
     *
     * @return number of commands replayed
     * @throws IOException if the file is corrupt in the middle (refuse to start rather than guess)
     */
    public static int load(Path path, Store store, CommandHandler handler) throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        int count = 0;
        long validEnd = 0;
        boolean tornTail = false;
        store.setLoading(true);
        try (CountingInputStream in = new CountingInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            RespParser parser = new RespParser(in);
            ByteArrayOutputStream sink = new ByteArrayOutputStream(); // replies are thrown away
            while (true) {
                List<byte[]> command;
                try {
                    command = parser.readCommand();
                } catch (EOFException e) {
                    tornTail = true; // file ends in the middle of a command: the crash tore the last write
                    break;
                } catch (RespParser.ProtocolException e) {
                    throw new IOException("AOF is corrupted near byte " + in.count() + ": " + e.getMessage(), e);
                }
                if (command == null) {
                    break;
                }
                if (!command.isEmpty()) {
                    sink.reset();
                    handler.execute(command, sink);
                    count++;
                }
                validEnd = in.count();
            }
        } finally {
            store.setLoading(false);
        }
        if (tornTail) {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
                channel.truncate(validEnd); // drop the half-written command so new appends start clean
            }
            System.err.println("AOF: dropped an incomplete trailing command, file truncated to " + validEnd + " bytes");
        }
        return count;
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** Counts bytes consumed, so we know exactly where the last COMPLETE command ended. */
    private static final class CountingInputStream extends FilterInputStream {
        private long count;

        CountingInputStream(InputStream in) {
            super(in);
        }

        long count() {
            return count;
        }

        @Override
        public int read() throws IOException {
            int b = super.read();
            if (b != -1) {
                count++;
            }
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            int n = super.read(buf, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }
    }
}
