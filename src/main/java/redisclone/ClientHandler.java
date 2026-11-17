package redisclone;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * Serves one connected client: read a command, run it, write the reply, repeat.
 * One of these runs on its own thread per connection.
 *
 * The reply is first built in memory (inside the engine's lock), then copied to the socket
 * here, OUTSIDE the lock. So a slow or stuck client can never block command execution.
 */
public final class ClientHandler implements Runnable {

    private final Socket socket;
    private final CommandHandler commands;

    public ClientHandler(Socket socket, CommandHandler commands) {
        this.socket = socket;
        this.commands = commands;
    }

    @Override
    public void run() {
        try (socket;
             BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream())) {

            socket.setTcpNoDelay(true);
            RespParser parser = new RespParser(in);
            ByteArrayOutputStream reply = new ByteArrayOutputStream();

            while (true) {
                List<byte[]> command;
                try {
                    command = parser.readCommand();
                } catch (RespParser.ProtocolException e) {
                    RespWriter.error(out, "ERR Protocol error: " + e.getMessage());
                    out.flush();
                    break; // real Redis also closes the connection after a protocol error
                }

                if (command == null) {
                    break; // client disconnected
                }
                if (command.isEmpty()) {
                    continue;
                }

                reply.reset();
                boolean close = commands.execute(command, reply);
                reply.writeTo(out);

                // Pipelining: if more commands are already buffered, answer them all, then flush once.
                if (close || in.available() == 0) {
                    out.flush();
                }
                if (close) {
                    break;
                }
            }
        } catch (IOException e) {
            // Connection reset / broken pipe / EOF mid-command: just drop this client.
        }
    }
}
