#!/usr/bin/env bash
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
# Bring up (or tear down) the Docker environment for the Kerberos E2E test:
#   1. krb-test-net            — dedicated bridge network
#   2. kdc-server              — MIT Kerberos KDC (realm S2.TEST)
#   3. singlestore-integration — SingleStore with GSSAPI auth + keytab from KDC
#   4. krb-client              — JDK 21 container with /jdbc bind-mounted
#
# After this script completes successfully, run `run-kerberos-tests.sh` to
# build the JDBC driver and exercise it against the running stack.
#
# Usage:
#   export SINGLESTORE_LICENSE="<your-license>"
#   export ROOT_PASSWORD="password"
#   ./scripts/kerberos/setup-kerberos.sh           # bring the stack up
#   ./scripts/kerberos/setup-kerberos.sh teardown  # remove containers + network
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

# ---------------------------------------------------------------------------
# Teardown
# ---------------------------------------------------------------------------
teardown() {
    log "Tearing down containers and network..."
    docker rm -f "$KDC_CONTAINER" "$CLIENT_CONTAINER" 2>/dev/null || true
    # leave singlestore-integration alone if caller wants to keep it
    if [ "${TEARDOWN_S2:-1}" = "1" ]; then
        docker rm -f "$S2_CONTAINER" 2>/dev/null || true
    fi
    docker network rm "$NETWORK_NAME" 2>/dev/null || true
    log "Teardown complete."
}

if [ "${1:-}" = "teardown" ]; then teardown; exit 0; fi

SINGLESTORE_LICENSE="${SINGLESTORE_LICENSE:?Set SINGLESTORE_LICENSE}"

# ---------------------------------------------------------------------------
# Step 1: Create Docker network
# ---------------------------------------------------------------------------
log "STEP 1: Create Docker network '${NETWORK_NAME}'"
docker network inspect "$NETWORK_NAME" >/dev/null 2>&1 || \
    docker network create "$NETWORK_NAME"

# ---------------------------------------------------------------------------
# Step 2: Build & start KDC container
# ---------------------------------------------------------------------------
log "STEP 2: Build and start KDC container"
docker rm -f "$KDC_CONTAINER" 2>/dev/null || true

docker build -t kdc-image -f "${SCRIPT_DIR}/Dockerfile.kdc" "${SCRIPT_DIR}"

docker run -d \
    --name "$KDC_CONTAINER" \
    --hostname kdc.s2.test \
    --network "$NETWORK_NAME" \
    kdc-image

wait_for_container "$KDC_CONTAINER" 30

log "Waiting for KDC to generate keytabs..."
for i in $(seq 1 30); do
    if docker exec "$KDC_CONTAINER" test -f /keytabs/singlestore.keytab 2>/dev/null; then
        break
    fi
    sleep 1
done
docker exec "$KDC_CONTAINER" test -f /keytabs/singlestore.keytab || \
    fail "KDC did not generate keytabs"

log "KDC is ready. Keytabs generated."

# ---------------------------------------------------------------------------
# Step 3: Start SingleStore container
# ---------------------------------------------------------------------------
log "STEP 3: Start SingleStore container"

S2_EXISTS=$(docker inspect "$S2_CONTAINER" >/dev/null 2>&1 && echo 1 || echo 0)
if [ "$S2_EXISTS" = "1" ]; then
    log "SingleStore container already exists — connecting to network"
    docker network connect "$NETWORK_NAME" "$S2_CONTAINER" 2>/dev/null || true
else
    s2_env_args=(
        -e "SINGLESTORE_LICENSE=${SINGLESTORE_LICENSE}"
        -e "ROOT_PASSWORD=${ROOT_PASSWORD}"
    )
    if [ -n "${SINGLESTORE_VERSION}" ]; then
        s2_env_args+=(-e "SINGLESTORE_VERSION=${SINGLESTORE_VERSION}")
    fi

    docker run -d \
        --name "$S2_CONTAINER" \
        --hostname "$S2_CONTAINER" \
        --network "$NETWORK_NAME" \
        "${s2_env_args[@]}" \
        -p ${S2_PORT}:3306 -p 5507:3307 -p 5508:3308 \
        "$S2_IMAGE"
fi

wait_for_singlestore 120

# ---------------------------------------------------------------------------
# Step 4: Copy keytab from KDC to SingleStore and configure GSSAPI
# ---------------------------------------------------------------------------
log "STEP 4: Copy keytab to SingleStore and configure GSSAPI"

docker cp "${KDC_CONTAINER}:/keytabs/singlestore.keytab" "/tmp/singlestore.keytab"
docker cp "/tmp/singlestore.keytab" "${S2_CONTAINER}:/singlestore.keytab"
docker cp "${SCRIPT_DIR}/krb5.conf" "${S2_CONTAINER}:/etc/krb5.conf"

docker exec -u 0 "$S2_CONTAINER" bash -c '
    { yum -y install krb5-workstation krb5-libs 2>/dev/null; } || \
    { apt-get update && apt-get install -y krb5-user; } 2>/dev/null || true
'

docker exec "$S2_CONTAINER" memsqlctl update-config --yes --all \
    --key gssapi_keytab_path --value /singlestore.keytab
docker exec "$S2_CONTAINER" memsqlctl update-config --yes --all \
    --key gssapi_principal_name --value "${KRB_SERVICE_PRINCIPAL}@${REALM}"

docker restart "$S2_CONTAINER"
wait_for_singlestore 120

# ---------------------------------------------------------------------------
# Step 5: Create Kerberos-authenticated user in SingleStore
# ---------------------------------------------------------------------------
log "STEP 5: Create Kerberos-authenticated user in SingleStore"

docker exec "$S2_CONTAINER" memsql -u root -p"${ROOT_PASSWORD}" -e "
    DROP USER IF EXISTS '${KRB_CLIENT_PRINCIPAL}'@'%';
    CREATE USER '${KRB_CLIENT_PRINCIPAL}'@'%' IDENTIFIED WITH authentication_gss AS '${KRB_CLIENT_PRINCIPAL}@${REALM}';
    GRANT ALL PRIVILEGES ON *.* TO '${KRB_CLIENT_PRINCIPAL}'@'%';
    DROP USER IF EXISTS '${KRB_IMPERSONATED_PRINCIPAL}'@'%';
    CREATE USER '${KRB_IMPERSONATED_PRINCIPAL}'@'%' IDENTIFIED WITH authentication_gss AS '${KRB_IMPERSONATED_PRINCIPAL}@${REALM}';
    GRANT ALL PRIVILEGES ON *.* TO '${KRB_IMPERSONATED_PRINCIPAL}'@'%';
    CREATE DATABASE IF NOT EXISTS test;
"

log "Users '${KRB_CLIENT_PRINCIPAL}' and '${KRB_IMPERSONATED_PRINCIPAL}' created with GSSAPI auth."

# ---------------------------------------------------------------------------
# Step 6: Build & start Kerberos client container
# ---------------------------------------------------------------------------
log "STEP 6: Build and start Kerberos client container"
docker rm -f "$CLIENT_CONTAINER" 2>/dev/null || true

docker build -t krb-client-image -f "${SCRIPT_DIR}/Dockerfile.client" "${SCRIPT_DIR}"

docker run -d \
    --name "$CLIENT_CONTAINER" \
    --hostname krb-client.s2.test \
    --network "$NETWORK_NAME" \
    -v "${REPO_ROOT}:/jdbc" \
    krb-client-image \
    sleep infinity

wait_for_container "$CLIENT_CONTAINER" 15

docker exec "$CLIENT_CONTAINER" mkdir -p /keytabs
docker cp "${KDC_CONTAINER}:/keytabs/client.keytab" "/tmp/client.keytab"
docker cp "/tmp/client.keytab" "${CLIENT_CONTAINER}:/keytabs/client.keytab"
docker cp "${SCRIPT_DIR}/krb5.conf" "${CLIENT_CONTAINER}:/etc/krb5.conf"
docker cp "${SCRIPT_DIR}/jaas.conf" "${CLIENT_CONTAINER}:/jaas.conf"

# ---------------------------------------------------------------------------
# Step 7: kinit on client — verify Kerberos ticket acquisition
# ---------------------------------------------------------------------------
log "STEP 7: Acquire Kerberos ticket on client (kinit)"

docker exec "$CLIENT_CONTAINER" kinit -kt /keytabs/client.keytab "${KRB_CLIENT_PRINCIPAL}@${REALM}"
docker exec "$CLIENT_CONTAINER" klist

log "Kerberos ticket acquired successfully."
log "Environment is ready. Run scripts/kerberos/run-kerberos-tests.sh to execute the tests."
