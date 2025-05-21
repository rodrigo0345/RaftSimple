#!/usr/bin/env bash
export FAULTY_FOLLOWER=1
BASEDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${BASEDIR}/../RaftSimple/run_fault.sh" "$@"
