#!/usr/bin/env bash

set -eu

echo "=== Disk usage BEFORE cleanup ==="
df -h

sudo rm -rf /usr/share/dotnet
sudo rm -rf /opt/ghc
sudo rm -rf /usr/local/share/boost
if [ -n "${AGENT_TOOLSDIRECTORY:-}" ]; then
    sudo rm -rf "${AGENT_TOOLSDIRECTORY}"
fi
sudo rm -rf /usr/local/lib/android
sudo rm -rf /opt/hostedtoolcache/CodeQL
sudo rm -rf /opt/hostedtoolcache/Ruby
sudo rm -rf /opt/hostedtoolcache/Go
docker system prune -af || true
sudo apt-get clean || true

echo "=== Disk usage AFTER cleanup ==="
df -h
