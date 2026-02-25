#!/bin/bash
set -e
echo "=== CAV Worker Starting ==="
echo "PORT=$PORT"
echo "Python: $(python --version)"
echo "Working dir: $(pwd)"
echo "Files: $(ls -la)"
echo "==========================="
exec gunicorn app:app --bind "0.0.0.0:${PORT:-8090}" --workers 1 --timeout 120 --log-level info --access-logfile - --error-logfile -
