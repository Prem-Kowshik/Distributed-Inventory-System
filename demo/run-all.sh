#!/usr/bin/env bash
# Runs setup + all six demos without pauses.
cd "$(dirname "$0")/.."
export NOPAUSE=1
demo/00-setup.sh
for d in demo/0[1-6]-*.sh; do bash "$d"; done
