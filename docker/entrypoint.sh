#!/bin/sh
set -e

# Install extra dependencies from the mounted app directory.
if [ -f /app/requirements.txt ]; then
    pip install --no-cache-dir -r /app/requirements.txt
fi
if [ -f /app/pyproject.toml ]; then
    pip install --no-cache-dir /app/
fi

# No CLI args: auto-detect or show help.
if [ $# -eq 0 ]; then
    if [ -f /app/main.py ]; then
        exec kopf run -v /app/main.py
    else
        cat /usr/local/share/kopf/usage.txt
        exit 1
    fi
fi

# CLI args provided: pass through to kopf.
exec kopf "$@"
