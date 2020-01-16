#!/bin/bash

MYDIR="$(dirname $0)"
pushd "${MYDIR}" >/dev/null
[[ -d .venv ]] && source .venv/bin/activate
python -m zfsbackup.cli "$@"
popd >/dev/null
