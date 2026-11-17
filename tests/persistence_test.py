"""
Crash-recovery test. Starts real server processes, kills them with SIGKILL (no clean shutdown,
no chance to flush anything) and checks that a fresh process recovers the data.

    python3 tests/persistence_test.py          (run from the project root, after compiling to ./out)
"""
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

from resp import Checker, Client

PORT = 6391
t = Checker()
check = t.check
DATA_DIR = tempfile.mkdtemp(prefix="redis-clone-aof-")
AOF = os.path.join(DATA_DIR, "appendonly.aof")


def start(fsync="always"):
    p = subprocess.Popen(
        ["java", "-cp", "out", "redisclone.Main", "--port", str(PORT), "--appendonly", "yes",
         "--dir", DATA_DIR, "--appendfsync", fsync],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", PORT)).close()
            return p
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("server did not start")


def crash(p):
    p.send_signal(signal.SIGKILL)   # like a power cut for the process: no shutdown hooks run
    p.wait()


print("1) write data, kill -9, restart")
p = start()
c = Client(PORT)
c.cmd("SET", "a", "1"); c.cmd("INCR", "a")
c.cmd("RPUSH", "l", "x", "y", "z"); c.cmd("LPOP", "l")
c.cmd("SET", "temp", "v", "PX", 600_000)
c.cmd("SET", "gone", "1"); c.cmd("DEL", "gone")
crash(p)
p = start()
c = Client(PORT)
check("counter survived", c.cmd("GET", "a"), b"2")
check("list survived", c.cmd("LRANGE", "l", 0, -1), [b"y", b"z"])
check("TTL survived", 0 < c.cmd("PTTL", "temp") <= 600_000, True)
check("deleted key stays deleted", c.cmd("EXISTS", "gone"), 0)

print("2) crash in the middle of a write (torn tail)")
crash(p)
good = os.path.getsize(AOF)
with open(AOF, "ab") as f:
    f.write(b"*3\r\n$3\r\nSET\r\n$4\r\nhalf\r\n$10\r\nabc")   # incomplete command
p = start()
c = Client(PORT)
check("data before the torn write is intact", c.cmd("GET", "a"), b"2")
check("torn command not applied", c.cmd("GET", "half"), None)
check("file repaired on startup", os.path.getsize(AOF), good)
check("server accepts writes after repair", c.cmd("SET", "after", "ok"), ("+", "OK"))

print("3) log compaction")
for i in range(300):
    c.cmd("SET", "hot", i)
before = os.path.getsize(AOF)
check("REWRITEAOF", c.cmd("REWRITEAOF"), ("+", "OK"))
after = os.path.getsize(AOF)
check("log got smaller", after < before, True)
c.cmd("SET", "post", "rewrite")
crash(p)
p = start()
c = Client(PORT)
check("hot key recovered after rewrite", c.cmd("GET", "hot"), b"299")
check("list recovered after rewrite", c.cmd("LRANGE", "l", 0, -1), [b"y", b"z"])
check("write after rewrite recovered", c.cmd("GET", "post"), b"rewrite")

print("4) expire -> re-create -> crash")
c.cmd("SET", "s", "5", "PX", 400)
c.cmd("INCR", "s")
time.sleep(0.7)
check("expired", c.cmd("GET", "s"), None)
c.cmd("SET", "s", "9")
crash(p)
p = start()
c = Client(PORT)
check("value after replay", c.cmd("GET", "s"), b"9")
check("no stale TTL after replay", c.cmd("TTL", "s"), -1)

print("5) everysec policy also survives a process crash")
c.cmd("SET", "es", "1")
crash(p)
p = start(fsync="everysec")
c = Client(PORT)
check("data written under always, read under everysec", c.cmd("GET", "es"), b"1")
c.cmd("SET", "es2", "2")
crash(p)
p = start(fsync="everysec")
c = Client(PORT)
check("everysec: process crash loses nothing (OS has the bytes)", c.cmd("GET", "es2"), b"2")
crash(p)

sys.exit(t.summary())
