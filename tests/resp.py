"""Tiny RESP client shared by the integration tests (raw sockets, no redis-cli needed)."""
import socket


def encode(*args):
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a if isinstance(a, bytes) else str(a).encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    return out


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.f = self.sock.makefile("rb")

    def send(self, data):
        self.sock.sendall(data)

    def reply(self):
        line = self.f.readline()
        if not line:
            return None
        kind, rest = line[:1], line[1:-2]
        if kind in (b"+", b"-"):
            return (kind.decode(), rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n == -1:
                return None
            data = self.f.read(n)
            self.f.read(2)
            return data
        if kind == b"*":
            return [self.reply() for _ in range(int(rest))]
        raise AssertionError(f"unexpected reply {line!r}")

    def cmd(self, *args):
        self.send(encode(*args))
        return self.reply()

    def close(self):
        self.sock.close()


class Checker:
    def __init__(self):
        self.passed = self.failed = 0

    def check(self, name, got, expected):
        if got == expected:
            self.passed += 1
            print(f"  ok    {name}")
        else:
            self.failed += 1
            print(f"  FAIL  {name}: expected {expected!r}, got {got!r}")

    def summary(self):
        print(f"\n{self.passed} passed, {self.failed} failed")
        return 1 if self.failed else 0
