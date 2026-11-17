package redisclone;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads client commands from a TCP byte stream using the RESP protocol.
 *
 * Key idea: TCP is a STREAM, not a sequence of messages. One read() may return
 * half a command, or three commands glued together. So we never assume message
 * boundaries; we read exactly what the protocol tells us to read
 * (a length prefix, then exactly that many bytes).
 *
 * A normal command arrives as an array of bulk strings:
 *   *2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n
 *
 * The InputStream passed in MUST be buffered (e.g. BufferedInputStream),
 * otherwise every in.read() is a separate system call.
 */
public final class RespParser {

    private static final int MAX_LINE = 64 * 1024;          // guard against endless lines
    private static final int MAX_BULK = 512 * 1024 * 1024;  // same limit real Redis uses (512 MB)
    private static final int MAX_ARGS = 1024 * 1024;

    /** Thrown when the client sends something that is not valid RESP. */
    public static final class ProtocolException extends IOException {
        private static final long serialVersionUID = 1L;

        public ProtocolException(String message) { super(message); }
    }

    private final InputStream in;

    public RespParser(InputStream in) {
        this.in = in;
    }

    /**
     * Reads the next command.
     *
     * @return the arguments (args.get(0) is the command name), an empty list for
     *         a blank/empty command, or null if the client closed the connection.
     */
    public List<byte[]> readCommand() throws IOException {
        int first = in.read();
        if (first == -1) {
            return null; // clean EOF: client disconnected between commands
        }
        if (first == '*') {
            return readArray();
        }
        return readInline(first);
    }

    private List<byte[]> readArray() throws IOException {
        long count = readLongLine();
        if (count > MAX_ARGS) {
            throw new ProtocolException("invalid multibulk length");
        }
        if (count <= 0) {
            return new ArrayList<>(); // "*0" or "*-1": nothing to execute
        }
        List<byte[]> args = new ArrayList<>((int) count);
        for (long i = 0; i < count; i++) {
            int type = in.read();
            if (type == -1) {
                throw new EOFException("connection closed in the middle of a command");
            }
            if (type != '$') {
                throw new ProtocolException("expected '$', got '" + (char) type + "'");
            }
            long len = readLongLine();
            if (len < 0 || len > MAX_BULK) {
                throw new ProtocolException("invalid bulk length");
            }
            byte[] data = in.readNBytes((int) len); // blocks until exactly len bytes (or EOF)
            if (data.length < len) {
                throw new EOFException("connection closed in the middle of a bulk string");
            }
            expectCrlf();
            args.add(data);
        }
        return args;
    }

    /** Supports typing "PING" into telnet/nc: a plain line split on whitespace. */
    private List<byte[]> readInline(int firstByte) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b = firstByte;
        while (b != '\n') {
            if (b == -1) {
                throw new EOFException("connection closed in the middle of a command");
            }
            line.write(b);
            if (line.size() > MAX_LINE) {
                throw new ProtocolException("too big inline request");
            }
            b = in.read();
        }
        String text = line.toString("UTF-8").trim();
        List<byte[]> args = new ArrayList<>();
        if (!text.isEmpty()) {
            for (String part : text.split("\\s+")) {
                args.add(part.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        return args;
    }

    /** Reads up to and including "\r\n" and parses what came before it as a number. */
    private long readLongLine() throws IOException {
        String line = readLine();
        try {
            return Long.parseLong(line);
        } catch (NumberFormatException e) {
            throw new ProtocolException("invalid number '" + line + "'");
        }
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int b = in.read();
            if (b == -1) {
                throw new EOFException("connection closed in the middle of a command");
            }
            if (b == '\r') {
                int next = in.read();
                if (next == -1) {
                    throw new EOFException("connection closed in the middle of a command");
                }
                if (next != '\n') {
                    throw new ProtocolException("expected LF after CR");
                }
                return sb.toString();
            }
            sb.append((char) b);
            if (sb.length() > MAX_LINE) {
                throw new ProtocolException("too big line");
            }
        }
    }

    private void expectCrlf() throws IOException {
        int cr = in.read();
        if (cr == -1) {
            throw new EOFException("connection closed in the middle of a bulk string");
        }
        int lf = in.read();
        if (lf == -1) {
            throw new EOFException("connection closed in the middle of a bulk string");
        }
        if (cr != '\r' || lf != '\n') {
            throw new ProtocolException("bulk string not terminated by CRLF");
        }
    }
}
