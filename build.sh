#!/usr/bin/env bash
# Compiles all sources into ./out (needs JDK 17+)
set -e
cd "$(dirname "$0")"
rm -rf out && mkdir -p out
find src -name '*.java' > out/sources.txt
javac --release 17 -d out @out/sources.txt
echo "Build OK -> out/"
