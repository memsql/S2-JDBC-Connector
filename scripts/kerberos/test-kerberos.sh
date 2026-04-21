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
# End-to-end Kerberos authentication test for the SingleStore JDBC Connector.
#
# Architecture:
#   1. kdc-server              — MIT Kerberos KDC (realm S2.TEST)
#   2. singlestore-integration — SingleStore with GSSAPI auth + keytab from KDC
#   3. krb-client              — JDK 21 container that runs kinit + JDBC connection test
#
# Usage:
#   export SINGLESTORE_LICENSE="<your-license>"
#   export ROOT_PASSWORD="password"
#   ./scripts/kerberos/test-kerberos.sh           # rebuild JDBC JAR and re-run the test (default)
#   ./scripts/kerberos/test-kerberos.sh startup   # run all setup steps, then build and run the test
#   ./scripts/kerberos/test-kerberos.sh teardown  # clean up containers/network
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

STARTUP=0
if [ "${1:-}" = "startup" ]; then STARTUP=1; fi

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
if [ "$STARTUP" = "1" ]; then
    SINGLESTORE_LICENSE="${SINGLESTORE_LICENSE:?Set SINGLESTORE_LICENSE}"
else
    SINGLESTORE_LICENSE="${SINGLESTORE_LICENSE:-}"
fi
SINGLESTORE_VERSION="${SINGLESTORE_VERSION:-9.0}"
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

if [ "$STARTUP" = "1" ]; then

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
    docker run -d \
        --name "$S2_CONTAINER" \
        --hostname "$S2_CONTAINER" \
        --network "$NETWORK_NAME" \
        -e SINGLESTORE_LICENSE="${SINGLESTORE_LICENSE}" \
        -e ROOT_PASSWORD="${ROOT_PASSWORD}" \
        -e SINGLESTORE_VERSION="${SINGLESTORE_VERSION}" \
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
    yum -y install krb5-workstation krb5-libs 2>/dev/null || \
    apt-get update && apt-get install -y krb5-user 2>/dev/null || true
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

else
    docker inspect "$CLIENT_CONTAINER" >/dev/null 2>&1 || \
        fail "default (repeat) mode requires an existing '${CLIENT_CONTAINER}' container — run '$0 startup' first"
    log "Skipping STEPS 1-7 (infrastructure setup), refreshing Kerberos ticket"
    docker exec "$CLIENT_CONTAINER" kinit -kt /keytabs/client.keytab "${KRB_CLIENT_PRINCIPAL}@${REALM}"
fi

# ---------------------------------------------------------------------------
# Step 8: Build the JDBC driver JAR inside the client container and compile
#         the Java test harness (KerberosIntegrationTest)
# ---------------------------------------------------------------------------
log "STEP 8: Build JDBC driver JAR"

docker exec -w /jdbc "$CLIENT_CONTAINER" mvn -B package -DskipTests -Dmaven.javadoc.skip=true -q

JDBC_JAR=$(docker exec -w /jdbc "$CLIENT_CONTAINER" \
    find target -maxdepth 1 -name 'singlestore-jdbc-client-*.jar' \
    ! -name '*-sources*' ! -name '*-javadoc*' ! -name '*-browser-sso*' \
    | head -1)

log "Built JDBC JAR: ${JDBC_JAR}"

log "Compiling Java test harness (KerberosIntegrationTest)"
docker cp "${SCRIPT_DIR}/KerberosIntegrationTest.java" \
    "${CLIENT_CONTAINER}:/tmp/KerberosIntegrationTest.java"
docker cp "${SCRIPT_DIR}/jaas.conf" "${CLIENT_CONTAINER}:/jaas.conf"
docker exec -w /jdbc "$CLIENT_CONTAINER" \
    javac /tmp/KerberosIntegrationTest.java -cp "/jdbc/${JDBC_JAR}" -d /tmp

# ---------------------------------------------------------------------------
# Test runner: remaining positional args are passed to `docker exec -e` as
# environment variables (KEY=VALUE) and consumed by KerberosIntegrationTest.
#
# Supported env vars (see KerberosIntegrationTest.java for details):
#   KRB_JDBC_URL             (string, required)
#   KRB_USER                 (string, required)
#   KRB_USE_GSS_CREDENTIAL   (boolean, optional, default false) — when true,
#                                                 requires KRB_IMPERSONATE_AS
#   KRB_IMPERSONATE_AS       (string, required when KRB_USE_GSS_CREDENTIAL=true)
#                                                 full principal to impersonate
#                                                 via constrained delegation
#   KRB_CONNECTION_ATTEMPTS  (integer, optional, default 1)
#   KRB_EXPECT_FAILURE       (boolean, optional, default false)
# ---------------------------------------------------------------------------
run_kerberos_test() {
    local env_args=()
    for kv in "$@"; do
        env_args+=("-e" "$kv")
    done

    docker exec -w /jdbc "${env_args[@]}" "$CLIENT_CONTAINER" bash -c "
        java \
            -Djava.security.auth.login.config=/jaas.conf \
            -Djava.security.krb5.conf=/etc/krb5.conf \
            -Djavax.security.auth.useSubjectCredsOnly=false \
            -cp /tmp:/jdbc/${JDBC_JAR} \
            KerberosIntegrationTest
    "
}

JDBC_URL_BASE="jdbc:singlestore://${S2_CONTAINER}:3306/test?servicePrincipalName=${KRB_SERVICE_PRINCIPAL}@${REALM}"

# ---------------------------------------------------------------------------
# Step 9: Basic Kerberos JDBC connection
# ---------------------------------------------------------------------------
log "STEP 9: Run JDBC Kerberos authentication test"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}"
log "STEP 9 PASSED"

# ---------------------------------------------------------------------------
# Step 10: Connect with requestCredentialDelegation=true
# ---------------------------------------------------------------------------
log "STEP 10: Run JDBC Kerberos test with requestCredentialDelegation=true"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}&requestCredentialDelegation=true" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}"
log "STEP 10 PASSED"

# ---------------------------------------------------------------------------
# Step 11: Connect with a custom jaasApplicationName
# ---------------------------------------------------------------------------
log "STEP 11: Run JDBC Kerberos test with jaasApplicationName=CustomKrbEntry"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}&jaasApplicationName=CustomKrbEntry" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}"
log "STEP 11 PASSED"

# ---------------------------------------------------------------------------
# Step 12: cacheJaasLoginContext=true (two sequential connections)
# ---------------------------------------------------------------------------
log "STEP 12: Run JDBC Kerberos test with cacheJaasLoginContext=true"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}&cacheJaasLoginContext=true" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}" \
    "KRB_CONNECTION_ATTEMPTS=2"
log "STEP 12 PASSED"

# ---------------------------------------------------------------------------
# Step 13: Negative — wrong servicePrincipalName must fail auth
# ---------------------------------------------------------------------------
log "STEP 13: Run JDBC Kerberos test with wrong servicePrincipalName (expect failure)"
run_kerberos_test \
    "KRB_JDBC_URL=jdbc:singlestore://${S2_CONTAINER}:3306/test?servicePrincipalName=wrong/${S2_CONTAINER}@${REALM}" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}" \
    "KRB_EXPECT_FAILURE=true"
log "STEP 13 PASSED"

# ---------------------------------------------------------------------------
# Step 14: Kerberos constrained delegation (S4U2Self + S4U2Proxy)
#
# The middle-tier principal (${KRB_CLIENT_PRINCIPAL}) logs in via JAAS, then
# uses ExtendedGSSCredential.impersonate() to mint a credential for
# ${KRB_IMPERSONATED_PRINCIPAL}. The JDBC driver receives that credential via
# the `gssCredential` connection property and establishes a GSS context to
# the SingleStore SPN — which triggers S4U2Proxy at the KDC. SingleStore
# must then see the session as the impersonated user.
# ---------------------------------------------------------------------------
log "STEP 14: Run JDBC Kerberos test with constrained delegation (S4U2Self/S4U2Proxy)"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}" \
    "KRB_USER=${KRB_IMPERSONATED_PRINCIPAL}" \
    "KRB_USE_GSS_CREDENTIAL=true" \
    "KRB_IMPERSONATE_AS=${KRB_IMPERSONATED_PRINCIPAL}@${REALM}"
log "STEP 14 PASSED"

echo ""
log "=========================================="
log "  KERBEROS E2E TEST PASSED"
log "=========================================="
