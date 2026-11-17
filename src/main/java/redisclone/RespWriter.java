package redisclone;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Encodes replies in RESP. Each method writes one complete reply.
 *
 *   +OK\r\n              simple string
 *   -ERR something\r\n   error
 *   :42\r\n              integer
 *   $5\r\nhello\r\n      bulk string (length-prefixed, binary safe)
 *   $-1\r\n              null bulk string ("no such key")
 */
public final class RespWriter {

    private static final byte[] CRLF = {'\r', '\n'};

    private RespWriter() {}

    public static void simpleString(OutputStream out, String s) throws IOException {
        out.write('+');
        out.write(sanitize(s).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    public static void error(OutputStream out, String message) throws IOException {
        out.write('-');
        out.write(sanitize(message).getBytes(StandardCharsets.UTF_8));
        out.write(CRLF);
    }

    public static void integer(OutputStream out, long value) throws IOException {
        out.write(':');
        out.write(Long.toString(value).getBytes(StandardCharsets.US_ASCII));
        out.write(CRLF);
    }

    public static void bulkString(OutputStream out, byte[] data) throws IOException {
        out.write('$');
        out.write(Integer.toString(data.length).getBytes(StandardCharsets.US_ASCII));
        out.write(CRLF);
        out.write(data);
        out.write(CRLF);
    }

    public static void nullBulkString(OutputStream out) throws IOException {
        out.write("$-1\r\n".getBytes(StandardCharsets.US_ASCII));
    }

    /** Header of an array reply; the caller then writes {@code count} elements. */
    public static void arrayHeader(OutputStream out, int count) throws IOException {
        out.write('*');
        out.write(Integer.toString(count).getBytes(StandardCharsets.US_ASCII));
        out.write(CRLF);
    }

    /** Encodes a command exactly as a client would send it. The AOF file is just these, back to back. */
    public static byte[] encodeCommand(List<byte[]> args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            arrayHeader(out, args.size());
            for (byte[] arg : args) {
                bulkString(out, arg);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e); // cannot happen for an in-memory stream
        }
        return out.toByteArray();
    }

    /** Simple strings and errors must stay on one line, so strip any CR/LF. */
    private static String sanitize(String s) {
        return s.replace('\r', ' ').replace('\n', ' ');
    }
}
