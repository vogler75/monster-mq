#!/bin/bash
cd "$(dirname "$0")"
exec ./build.sh -c -t -n "$@"
