#!/usr/bin/env bash
set -eu

REALM="S2.TEST"
KDC_PASSWORD="masterkey123"
S2_HOST="singlestore-integration"
CLIENT_PRINCIPAL="test_krb_user@${REALM}"
S2_SERVICE_PRINCIPAL="singlestore/${S2_HOST}@${REALM}"

echo "=== Creating KDC ACL file ==="
mkdir -p /etc/krb5kdc
echo "*/admin@${REALM} *" > /etc/krb5kdc/kadm5.acl

echo "=== Creating KDC database ==="
kdb5_util create -s -r "${REALM}" -P "${KDC_PASSWORD}"

echo "=== Starting KDC ==="
krb5kdc

echo "=== Creating principals ==="
kadmin.local -q "addprinc -pw clientpass ${CLIENT_PRINCIPAL}"
kadmin.local -q "addprinc -randkey ${S2_SERVICE_PRINCIPAL}"

echo "=== Exporting keytabs ==="
mkdir -p /keytabs

kadmin.local -q "ktadd -k /keytabs/singlestore.keytab ${S2_SERVICE_PRINCIPAL}"
kadmin.local -q "ktadd -k /keytabs/client.keytab ${CLIENT_PRINCIPAL}"

chmod 644 /keytabs/*.keytab

echo "=== KDC ready ==="
echo "  Realm:              ${REALM}"
echo "  Client principal:   ${CLIENT_PRINCIPAL}"
echo "  Service principal:  ${S2_SERVICE_PRINCIPAL}"
echo "  Keytabs in:         /keytabs/"

tail -f /var/log/krb5kdc.log 2>/dev/null || exec sleep infinity
