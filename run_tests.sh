#!/usr/bin/env bash
# Compiles everything and runs all three test layers. Needs JDK 17+ and python3.
set -euo pipefail
cd "$(dirname "$0")"

rm -rf out && mkdir out
javac -d out $(find src -name '*.java')

echo "== unit tests =="
java -cp out redisclone.UnitTests

echo; echo "== integration tests (real TCP) =="
java -cp out redisclone.Main --port 6380 > /dev/null 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true' EXIT
sleep 1.5
(cd tests && python3 smoke_test.py 6380)

echo; echo "== crash recovery (kill -9) =="
PYTHONPATH=tests python3 tests/persistence_test.py
