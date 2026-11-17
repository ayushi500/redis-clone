"""
End-to-end test over real TCP.   1) java -cp out redisclone.Main --port 6380
                                 2) python3 tests/smoke_test.py 6380
"""
import sys
import threading
import time

from resp import Checker, Client, encode

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 6379
t = Checker()
check = t.check
c = Client(PORT)
c.cmd("FLUSHALL")

print("basic commands")
check("PING", c.cmd("PING"), ("+", "PONG"))
check("PING msg", c.cmd("PING", "hi"), b"hi")
check("ECHO", c.cmd("ECHO", "hello"), b"hello")
check("SET", c.cmd("SET", "name", "ayushi"), ("+", "OK"))
check("GET", c.cmd("GET", "name"), b"ayushi")
check("GET missing -> null", c.cmd("GET", "nope"), None)
check("EXISTS", c.cmd("EXISTS", "name", "nope"), 1)
c.cmd("SET", "a", "1"); c.cmd("SET", "b", "2")
check("DEL multiple", c.cmd("DEL", "a", "b", "missing"), 2)
check("lowercase command", c.cmd("ping"), ("+", "PONG"))
check("unknown command", c.cmd("FLY")[0], "-")
check("wrong arity GET", c.cmd("GET")[0], "-")
check("COMMAND returns empty array (redis-cli compat)", c.cmd("COMMAND"), [])
check("QUIT", c.cmd("QUIT"), ("+", "OK"))
c = Client(PORT)

print("counters")
check("INCR", c.cmd("INCR", "hits"), 1)
check("INCRBY", c.cmd("INCRBY", "hits", 9), 10)
check("DECR", c.cmd("DECR", "hits"), 9)
c.cmd("SET", "word", "abc")
check("INCR on text -> error", c.cmd("INCR", "word")[0], "-")

print("expiry (real clock)")
check("SET with PX", c.cmd("SET", "short", "v", "PX", 300), ("+", "OK"))
check("alive immediately", c.cmd("GET", "short"), b"v")
ttl = c.cmd("PTTL", "short")
check("PTTL within range", 0 < ttl <= 300, True)
time.sleep(0.5)
check("gone after deadline", c.cmd("GET", "short"), None)
c.cmd("SET", "unread", "v", "PX", 200)
size_before = c.cmd("DBSIZE")
time.sleep(0.6)   # nobody reads 'unread': the background sweeper must remove it
check("active expiry removes untouched key", c.cmd("DBSIZE"), size_before - 1)
c.cmd("SET", "k", "v")
check("EXPIRE", c.cmd("EXPIRE", "k", 100), 1)
check("TTL", c.cmd("TTL", "k") in (99, 100), True)
check("PERSIST", c.cmd("PERSIST", "k"), 1)
check("TTL no expiry", c.cmd("TTL", "k"), -1)

print("lists")
check("RPUSH", c.cmd("RPUSH", "l", "a", "b", "c"), 3)
check("LPUSH", c.cmd("LPUSH", "l", "z"), 4)
check("LRANGE", c.cmd("LRANGE", "l", 0, -1), [b"z", b"a", b"b", b"c"])
check("LPOP", c.cmd("LPOP", "l"), b"z")
check("RPOP", c.cmd("RPOP", "l"), b"c")
check("LLEN", c.cmd("LLEN", "l"), 2)
check("WRONGTYPE", c.cmd("GET", "l")[1].startswith("WRONGTYPE"), True)

print("protocol / framing")
payload = b"line1\r\nline2\r\n$*+-"
check("binary-safe value", (c.cmd("SET", "bin", payload), c.cmd("GET", "bin")), (("+", "OK"), payload))
big = b"x" * 200_000
c.cmd("SET", "big", big)
check("large value (200KB)", c.cmd("GET", "big"), big)
c2 = Client(PORT)
msg = encode("PING")
c2.send(msg[:7]); time.sleep(0.2); c2.send(msg[7:])
check("fragmented command", c2.reply(), ("+", "PONG"))
c3 = Client(PORT)
c3.send(encode("SET", "p", "1") + encode("GET", "p") + encode("PING"))
check("pipelined (3 replies)", (c3.reply(), c3.reply(), c3.reply()), (("+", "OK"), b"1", ("+", "PONG")))
c4 = Client(PORT)
c4.send(b"PING\r\n")
check("inline command", c4.reply(), ("+", "PONG"))
c5 = Client(PORT)
c5.send(b"*1\r\n#oops\r\n")
check("protocol error -> error reply", c5.reply()[0], "-")
check("protocol error -> connection closed", c5.reply(), None)

print("concurrency")
errors = []


def worker(i):
    try:
        w = Client(PORT)
        for j in range(200):
            key, val = f"w{i}:{j}", f"v{i}:{j}"
            assert w.cmd("SET", key, val) == ("+", "OK")
            assert w.cmd("GET", key) == val.encode()
    except Exception as e:  # noqa
        errors.append(e)


threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
[x.start() for x in threads]
[x.join() for x in threads]
check("50 clients x 200 SET/GET each", errors, [])

c.cmd("DEL", "shared")
errors.clear()


def incr_worker():
    try:
        w = Client(PORT)
        for _ in range(200):
            w.cmd("INCR", "shared")
    except Exception as e:  # noqa
        errors.append(e)


threads = [threading.Thread(target=incr_worker) for _ in range(20)]
[x.start() for x in threads]
[x.join() for x in threads]
check("20 clients x 200 INCR = exactly 4000 (atomic)", c.cmd("GET", "shared"), b"4000")

sys.exit(t.summary())
