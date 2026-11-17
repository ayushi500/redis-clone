package redisclone;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Minimal load generator in the spirit of redis-benchmark.
 *
 *   java -cp out redisclone.Benchmark [port] [clients] [totalRequests] [pipeline] [valueBytes]
 *   defaults:                          6379     50          100000        1          16
 *
 * Reports throughput and latency per round trip (one pipeline batch) for SET, then GET.
 * Compare against the real thing:  redis-benchmark -p PORT -c 50 -n 100000 -P 1 -d 16 -t set,get
 * Only compare numbers measured on the same machine, in the same session.
 */
public final class Benchmark {

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 6379;
        int clients = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        int total = args.length > 2 ? Integer.parseInt(args[2]) : 100_000;
        int pipeline = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        int valueBytes = args.length > 4 ? Integer.parseInt(args[4]) : 16;

        String value = "x".repeat(valueBytes);
        System.out.printf("clients=%d total=%d pipeline=%d value=%dB%n", clients, total, pipeline, valueBytes);
        run("SET", port, clients, total, pipeline, value);
        run("GET", port, clients, total, pipeline, value);
    }

    private static void run(String op, int port, int clients, int total, int pipeline, String value)
            throws Exception {
        int perClient = total / clients;
        int batches = Math.max(1, perClient / pipeline);
        long[][] latencies = new long[clients][batches];
        CountDownLatch ready = new CountDownLatch(clients);
        CountDownLatch go = new CountDownLatch(1);
        Thread[] threads = new Thread[clients];
        Throwable[] failure = new Throwable[1];

        for (int c = 0; c < clients; c++) {
            final int id = c;
            threads[c] = new Thread(() -> {
                try (Socket s = new Socket("127.0.0.1", port)) {
                    s.setTcpNoDelay(true);
                    OutputStream out = s.getOutputStream();
                    InputStream in = new BufferedInputStream(s.getInputStream());
                    ready.countDown();
                    go.await();
                    for (int b = 0; b < batches; b++) {
                        StringBuilder sb = new StringBuilder();
                        for (int i = 0; i < pipeline; i++) {
                            String key = "key:" + ThreadLocalRandom.current().nextInt(100_000);
                            sb.append(op.equals("SET") ? command("SET", key, value) : command("GET", key));
                        }
                        long start = System.nanoTime();
                        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                        out.flush();
                        for (int i = 0; i < pipeline; i++) {
                            skipReply(in);
                        }
                        latencies[id][b] = System.nanoTime() - start;
                    }
                } catch (Throwable t) {
                    failure[0] = t;
                    ready.countDown();
                }
            });
            threads[c].start();
        }
        ready.await();
        long begin = System.nanoTime();
        go.countDown();
        for (Thread t : threads) {
            t.join();
        }
        double seconds = (System.nanoTime() - begin) / 1e9;
        if (failure[0] != null) {
            throw new RuntimeException("client failed", failure[0]);
        }

        long[] all = Arrays.stream(latencies).flatMapToLong(Arrays::stream).sorted().toArray();
        long requests = (long) clients * batches * pipeline;
        System.out.printf("%s: %d requests in %.2fs -> %.0f req/s | round trip p50=%.2fms p99=%.2fms max=%.2fms%n",
                op, requests, seconds, requests / seconds,
                all[all.length / 2] / 1e6, all[(int) (all.length * 0.99)] / 1e6, all[all.length - 1] / 1e6);
    }

    private static String command(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String p : parts) {
            sb.append('$').append(p.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(p).append("\r\n");
        }
        return sb.toString();
    }

    /** Reads and discards one reply (+simple, -error, :int, or $bulk). */
    private static void skipReply(InputStream in) throws IOException {
        int type = in.read();
        if (type == -1) {
            throw new IOException("server closed connection");
        }
        String line = readLine(in);
        if (type == '$') {
            int len = Integer.parseInt(line);
            if (len >= 0) {
                in.readNBytes(len + 2);
            }
        }
    }

    private static String readLine(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != '\r') {
            if (b == -1) {
                throw new IOException("eof");
            }
            sb.append((char) b);
        }
        in.read(); // '\n'
        return sb.toString();
    }
}
