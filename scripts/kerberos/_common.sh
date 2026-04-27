# ************************************************************************************
#   Copyright (c) 2026 SingleStore, Inc.
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
# Shared configuration and helpers for the Kerberos E2E test scripts.
# This file is meant to be sourced, not executed directly.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
REALM="S2.TEST"
NETWORK_NAME="krb-test-net"
KDC_CONTAINER="kdc-server"
S2_CONTAINER="singlestore-integration"
CLIENT_CONTAINER="krb-client"
S2_IMAGE="ghcr.io/singlestore-labs/singlestoredb-dev:latest"
S2_PORT=5506
ROOT_PASSWORD="${ROOT_PASSWORD:-password}"
SINGLESTORE_VERSION="${SINGLESTORE_VERSION:-}"
KRB_CLIENT_PRINCIPAL="test_krb_user"
KRB_IMPERSONATED_PRINCIPAL="impersonated_user"
KRB_SERVICE_PRINCIPAL="singlestore/${S2_CONTAINER}"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "=== [$(date +%H:%M:%S)] $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

wait_for_container() {
    local name="$1" timeout="${2:-60}"
    log "Waiting for container ${name} (up to ${timeout}s)..."
    local elapsed=0
    while [ $elapsed -lt "$timeout" ]; do
        local status
        status=$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || echo "missing")
        if [ "$status" = "exited" ] || [ "$status" = "dead" ]; then
            echo "--- Container ${name} logs ---" >&2
            docker logs "$name" 2>&1 | tail -50 >&2
            fail "Container ${name} exited unexpectedly (status=${status})"
        fi
        if [ "$status" = "running" ] && docker exec "$name" true 2>/dev/null; then
            return 0
        fi
        sleep 1; elapsed=$((elapsed + 1))
    done
    echo "--- Container ${name} logs ---" >&2
    docker logs "$name" 2>&1 | tail -50 >&2
    fail "Container ${name} not ready after ${timeout}s"
}

wait_for_singlestore() {
    local timeout="${1:-120}"
    log "Waiting for SingleStore to accept connections (up to ${timeout}s)..."
    local elapsed=0
    while [ $elapsed -lt "$timeout" ]; do
        if docker exec "$S2_CONTAINER" memsql -u root -p"${ROOT_PASSWORD}" -e "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1; elapsed=$((elapsed + 1))
    done
    fail "SingleStore not ready after ${timeout}s"
}
