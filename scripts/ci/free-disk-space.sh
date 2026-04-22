#!/usr/bin/env bash
# ************************************************************************************
#   Copyright (c) 2021-2025 SingleStore, Inc.
#
#   This library is free software; you can redistribute it and/or
#   modify it under the terms of the GNU Library General Public
#   License as published by the Free Software Foundation; either
#   version 2.1 of the License, or (at your option) any later version.
#
#   This library is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#   Library General Public License for more details.
#
#   You should have received a copy of the GNU Library General Public
#   License along with this library; if not see <http://www.gnu.org/licenses>
#   or write to the Free Software Foundation, Inc.,
#   51 Franklin St., Fifth Floor, Boston, MA 02110, USA
# *************************************************************************************/
#
# Remove pre-installed toolchains we don't use on GitHub-hosted runners so
# that disk-hungry jobs (SingleStore dev image, Kerberos KDC + client
# containers, Maven caches, etc.) have room to breathe.
#
# Safe to run on non-GitHub hosts: missing paths are silently skipped.

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
