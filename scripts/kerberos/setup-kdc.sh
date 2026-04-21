#!/usr/bin/env bash
set -eu

# Set up a Heimdal KDC for realm S2.TEST and export keytabs used by the
# JDBC Kerberos integration test.
#
# Heimdal is used (instead of MIT krb5) because MIT's DB2 KDB backend does
# not implement the KDB `check_allowed_to_delegate` callback, which makes
# classical S4U2Proxy (Kerberos constrained delegation) always fail with
# "UNSUPPORTED_S4U2PROXY_REQUEST". Heimdal supports S4U2Self + S4U2Proxy
# natively.

REALM="S2.TEST"
S2_HOST="singlestore-integration"
CLIENT_PRINCIPAL="test_krb_user@${REALM}"
IMPERSONATED_PRINCIPAL="impersonated_user@${REALM}"
S2_SERVICE_PRINCIPAL="singlestore/${S2_HOST}@${REALM}"
KDC_DB_DIR="/var/lib/heimdal-kdc"
KDC_LOG="/var/log/heimdal-kdc.log"

mkdir -p "${KDC_DB_DIR}" /var/log /keytabs /etc/heimdal-kdc

# kadmind ACL: root/admin can do anything.
cat > /etc/heimdal-kdc/kadmind.acl <<EOF
root/admin@${REALM} all
EOF

# kdc.conf — tell Heimdal where to keep its database and which realm to serve.
cat > /etc/heimdal-kdc/kdc.conf <<EOF
[kdc]
    database = {
        dbname = ${KDC_DB_DIR}/heimdal
        realm = ${REALM}
        mkey_file = ${KDC_DB_DIR}/m-key
        acl_file = /etc/heimdal-kdc/kadmind.acl
        log_file = ${KDC_LOG}
    }
EOF

echo "=== Initializing Heimdal KDC database (realm ${REALM}) ==="
kadmin -l init \
    --realm-max-ticket-life=1day \
    --realm-max-renewable-life=7day \
    "${REALM}"

echo "=== Creating principals ==="
kadmin -l add --password=clientpass  --use-defaults "${CLIENT_PRINCIPAL}"
kadmin -l add --password=imperspass  --use-defaults "${IMPERSONATED_PRINCIPAL}"
kadmin -l add --random-key          --use-defaults "${S2_SERVICE_PRINCIPAL}"

echo "=== Configuring constrained delegation on ${CLIENT_PRINCIPAL} ==="
# trusted-for-delegation  ≡ MIT's ok_to_auth_as_delegate   (enables S4U2Self
#                                                          protocol transition
#                                                          and makes the
#                                                          resulting evidence
#                                                          ticket forwardable).
# ok-as-delegate          ≡ MIT's ok_as_delegate           (sets the OK_AS_DELEGATE
#                                                          flag on issued tickets,
#                                                          a hint to clients).
# --constrained-delegation lists the allowed S4U2Proxy target services.
kadmin -l modify \
    --attributes=+ok-as-delegate,+trusted-for-delegation \
    --constrained-delegation="${S2_SERVICE_PRINCIPAL}" \
    "${CLIENT_PRINCIPAL}"

echo "=== Exporting keytabs to /keytabs/ ==="
# Heimdal writes MIT-format keytabs by default, so they are consumable by
# MIT krb5 clients (the krb-client container and SingleStore's GSSAPI plugin).
kadmin -l ext_keytab -k /keytabs/singlestore.keytab "${S2_SERVICE_PRINCIPAL}"
kadmin -l ext_keytab -k /keytabs/client.keytab      "${CLIENT_PRINCIPAL}"
chmod 644 /keytabs/*.keytab

echo "=== Heimdal KDC ready ==="
echo "  Realm:                     ${REALM}"
echo "  Client principal:          ${CLIENT_PRINCIPAL}"
echo "  Impersonated principal:    ${IMPERSONATED_PRINCIPAL}"
echo "  Service principal:         ${S2_SERVICE_PRINCIPAL}"
echo "  Keytabs in:                /keytabs/"

# Start the KDC in the foreground as PID 1 of this script so the container
# stays alive and docker restart policies behave correctly. Listen on all
# interfaces and on both UDP and TCP port 88 so sibling containers can reach
# us on the Docker network.
echo "=== Starting Heimdal KDC (0.0.0.0:88 udp+tcp) ==="
exec /usr/lib/heimdal-servers/kdc \
    --addresses=0.0.0.0 \
    --ports="88/udp 88/tcp"
