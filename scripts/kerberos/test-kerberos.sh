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
# Run the Kerberos JDBC E2E test scenarios against an already-running stack.
# Run scripts/kerberos/setup-kerberos.sh first to provision the containers.
#
# This script:
#   1. Refreshes the Kerberos ticket on the client container
#   2. Builds the JDBC driver JAR inside the client container
#   3. Compiles the KerberosIntegrationTest harness
#   4. Runs each test scenario (basic auth, credential delegation, custom
#      JAAS application name, login-context cache, negative SPN test, S4U
#      constrained delegation)
#
# Usage:
#   ./scripts/kerberos/test-kerberos.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=_common.sh
source "${SCRIPT_DIR}/_common.sh"

docker inspect "$CLIENT_CONTAINER" >/dev/null 2>&1 || \
    fail "client container '${CLIENT_CONTAINER}' not found — run scripts/kerberos/setup-kerberos.sh first"
docker inspect "$S2_CONTAINER" >/dev/null 2>&1 || \
    fail "SingleStore container '${S2_CONTAINER}' not found — run scripts/kerberos/setup-kerberos.sh first"

log "Refreshing Kerberos ticket on '${CLIENT_CONTAINER}'"
docker exec "$CLIENT_CONTAINER" kinit -kt /keytabs/client.keytab "${KRB_CLIENT_PRINCIPAL}@${REALM}"

# ---------------------------------------------------------------------------
# Build the JDBC driver JAR inside the client container and compile the
# Java test harness (KerberosIntegrationTest)
# ---------------------------------------------------------------------------
log "Build JDBC driver JAR"

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
# Test runner: positional args are forwarded to `docker exec -e` as
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
# Test 1: Basic Kerberos JDBC connection
# ---------------------------------------------------------------------------
log "TEST 1: Run JDBC Kerberos authentication test"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}"
log "TEST 1 PASSED"

# ---------------------------------------------------------------------------
# Test 2: Connect with requestCredentialDelegation=true
# ---------------------------------------------------------------------------
log "TEST 2: Run JDBC Kerberos test with requestCredentialDelegation=true"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}&requestCredentialDelegation=true" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}"
log "TEST 2 PASSED"

# ---------------------------------------------------------------------------
# Test 3: Connect with a custom jaasApplicationName
# ---------------------------------------------------------------------------
log "TEST 3: Run JDBC Kerberos test with jaasApplicationName=CustomKrbEntry"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}&jaasApplicationName=CustomKrbEntry" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}"
log "TEST 3 PASSED"

# ---------------------------------------------------------------------------
# Test 4: cacheJaasLoginContext=true (two sequential connections)
# ---------------------------------------------------------------------------
log "TEST 4: Run JDBC Kerberos test with cacheJaasLoginContext=true"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}&cacheJaasLoginContext=true" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}" \
    "KRB_CONNECTION_ATTEMPTS=2"
log "TEST 4 PASSED"

# ---------------------------------------------------------------------------
# Test 5: Negative — wrong servicePrincipalName must fail auth
# ---------------------------------------------------------------------------
log "TEST 5: Run JDBC Kerberos test with wrong servicePrincipalName (expect failure)"
run_kerberos_test \
    "KRB_JDBC_URL=jdbc:singlestore://${S2_CONTAINER}:3306/test?servicePrincipalName=wrong/${S2_CONTAINER}@${REALM}" \
    "KRB_USER=${KRB_CLIENT_PRINCIPAL}" \
    "KRB_EXPECT_FAILURE=true"
log "TEST 5 PASSED"

# ---------------------------------------------------------------------------
# Test 6: Kerberos constrained delegation (S4U2Self + S4U2Proxy)
#
# The middle-tier principal (${KRB_CLIENT_PRINCIPAL}) logs in via JAAS, then
# uses ExtendedGSSCredential.impersonate() to mint a credential for
# ${KRB_IMPERSONATED_PRINCIPAL}. The JDBC driver receives that credential via
# the `gssCredential` connection property and establishes a GSS context to
# the SingleStore SPN — which triggers S4U2Proxy at the KDC. SingleStore
# must then see the session as the impersonated user.
# ---------------------------------------------------------------------------
log "TEST 6: Run JDBC Kerberos test with constrained delegation (S4U2Self/S4U2Proxy)"
run_kerberos_test \
    "KRB_JDBC_URL=${JDBC_URL_BASE}" \
    "KRB_USER=${KRB_IMPERSONATED_PRINCIPAL}" \
    "KRB_USE_GSS_CREDENTIAL=true" \
    "KRB_IMPERSONATE_AS=${KRB_IMPERSONATED_PRINCIPAL}@${REALM}"
log "TEST 6 PASSED"

echo ""
log "=========================================="
log "  KERBEROS E2E TEST PASSED"
log "=========================================="
