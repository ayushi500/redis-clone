package redisclone;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Entry point.
 *
 *   java -cp out redisclone.Main [--port 6379] [--appendonly yes|no] [--dir .]
 *                                [--appendfilename appendonly.aof] [--appendfsync always|everysec|no]
 *
 * Startup order matters: (1) replay the AOF into an empty store, (2) only THEN open the file for
 * appending and start accepting clients, so replay never re-logs the commands it is replaying.
 */
public final class Main {

    private static final class Config {
        int port = 6379;
        boolean appendOnly = false;
        Path dir = Path.of(".");
        String aofFile = "appendonly.aof";
        Aof.Fsync fsync = Aof.Fsync.EVERYSEC;
    }

    public static void main(String[] args) throws IOException {
        Config cfg;
        try {
            cfg = parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println("usage: redisclone.Main [--port N] [--appendonly yes|no] [--dir PATH] "
                    + "[--appendfilename NAME] [--appendfsync always|everysec|no]");
            System.exit(2);
            return;
        }

        Store store = new Store();
        CommandHandler commands = new CommandHandler(store);

        Aof aof = null;
        if (cfg.appendOnly) {
            Files.createDirectories(cfg.dir);
            Path file = cfg.dir.resolve(cfg.aofFile);
            long start = System.nanoTime();
            int replayed = Aof.load(file, store, commands);
            System.out.printf("AOF loaded: %d commands, %d keys, %d ms%n",
                    replayed, store.size(), (System.nanoTime() - start) / 1_000_000);
            aof = new Aof(file, cfg.fsync);
            commands.attachAof(aof);
            final Aof toClose = aof;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    toClose.close(); // final fsync on Ctrl+C / SIGTERM
                } catch (IOException ignored) {
                    // nothing more we can do while shutting down
                }
            }));
        }

        // Active expiry: every 100 ms remove keys that are past their deadline.
        ScheduledExecutorService expirer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "active-expiry");
            t.setDaemon(true);
            return t;
        });
        expirer.scheduleWithFixedDelay(() -> {
            try {
                commands.activeExpireTick();
            } catch (Throwable t) { // an uncaught exception would silently cancel all future runs
                System.err.println("active expiry failed: " + t);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);

        ExecutorService pool = Executors.newCachedThreadPool();
        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true); // must be set BEFORE bind(); lets you restart quickly
            server.bind(new InetSocketAddress(cfg.port));
            System.out.println("redis-clone listening on port " + cfg.port
                    + (cfg.appendOnly ? " (AOF on, fsync=" + cfg.fsync.name().toLowerCase() + ")" : " (AOF off)"));
            while (true) {
                Socket client = server.accept();
                pool.execute(new ClientHandler(client, commands));
            }
        }
    }

    private static Config parse(String[] args) {
        Config c = new Config();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) {
                c.port = Integer.parseInt(a); // Stage-1 style: just a port number
                continue;
            }
            if (i + 1 >= args.length) {
                throw new IllegalArgumentException("missing value for " + a);
            }
            String v = args[++i];
            switch (a) {
                case "--port" -> c.port = Integer.parseInt(v);
                case "--appendonly" -> c.appendOnly = v.equalsIgnoreCase("yes");
                case "--dir" -> c.dir = Path.of(v);
                case "--appendfilename" -> c.aofFile = v;
                case "--appendfsync" -> c.fsync = Aof.Fsync.parse(v);
                default -> throw new IllegalArgumentException("unknown option " + a);
            }
        }
        return c;
    }
}
