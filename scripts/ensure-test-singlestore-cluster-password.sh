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

set -eu

# this script must be run from the top-level of the repo
cd "$(git rev-parse --show-toplevel)"


DEFAULT_SINGLESTORE_VERSION=""
VERSION="${SINGLESTORE_VERSION:-$DEFAULT_SINGLESTORE_VERSION}"
IMAGE_NAME="ghcr.io/singlestore-labs/singlestoredb-dev:latest"
CONTAINER_NAME="singlestore-integration"
SSL_DIR="${PWD}/scripts/ssl"

rm -rf "${SSL_DIR}"
mkdir -p "${SSL_DIR}"

echo "Create a Certificate Authority (CA)"

openssl genpkey -algorithm RSA -out "${SSL_DIR}"/ca-key.pem
openssl req -new -x509 -key  "${SSL_DIR}"/ca-key.pem -out "${SSL_DIR}"/ca-cert.pem -days 365 -subj "/CN=SingleStoreDBCA"

echo "Generate the Server Certificate"

openssl genpkey -algorithm RSA -out "${SSL_DIR}"/server-key.pem
openssl req -new -key "${SSL_DIR}"/server-key.pem -out "${SSL_DIR}"/server-req.csr -subj "/CN=singlestore-server"
openssl x509 -req -in "${SSL_DIR}"/server-req.csr -CA "${SSL_DIR}"/ca-cert.pem -CAkey "${SSL_DIR}"/ca-key.pem -CAcreateserial -out "${SSL_DIR}"/server-cert.pem -days 365

echo "Create truststore"
keytool -import -trustcacerts -file "${SSL_DIR}"/ca-cert.pem -keystore "${SSL_DIR}"/truststore.jks -storepass password -alias singlestore-ca -noprompt

echo "Generate the Client Certificate"

openssl genpkey -algorithm RSA -out "${SSL_DIR}"/client-key.pem
openssl req -new -key "${SSL_DIR}"/client-key.pem -out "${SSL_DIR}"/client-req.csr -subj "/CN=singlestore-client"
openssl x509 -req -in "${SSL_DIR}"/client-req.csr -CA "${SSL_DIR}"/ca-cert.pem -CAkey "${SSL_DIR}"/ca-key.pem -CAcreateserial -out "${SSL_DIR}"/client-cert.pem -days 365

echo "Create keystore"
openssl pkcs12 -export -inkey "${SSL_DIR}"/client-key.pem -in "${SSL_DIR}"/client-cert.pem -out "${SSL_DIR}"/client-keystore.p12 -name client-cert -CAfile "${SSL_DIR}"/ca-cert.pem -caname root -passout pass:password

chmod -R 777 "${SSL_DIR}"

EXISTS=$(docker inspect ${CONTAINER_NAME} >/dev/null 2>&1 && echo 1 || echo 0)

if [[ "${EXISTS}" -eq 1 ]]; then
  EXISTING_IMAGE_NAME=$(docker inspect -f '{{.Config.Image}}' ${CONTAINER_NAME})
  if [[ "${IMAGE_NAME}" != "${EXISTING_IMAGE_NAME}" ]]; then
    echo "Existing container ${CONTAINER_NAME} has image ${EXISTING_IMAGE_NAME} when ${IMAGE_NAME} is expected; recreating container."
    docker rm -f ${CONTAINER_NAME}
    EXISTS=0
  fi
fi

if [[ "${EXISTS}" -eq 0 ]]; then
    docker run -d \
        --name ${CONTAINER_NAME} \
        -v ${PWD}/scripts/ssl:/test-ssl \
        -v ${PWD}/scripts/jwt:/test-jwt \
        -e SINGLESTORE_LICENSE=${SINGLESTORE_LICENSE} \
        -e ROOT_PASSWORD=${ROOT_PASSWORD} \
        -e SINGLESTORE_VERSION=${VERSION} \
        -p 5506:3306 -p 5507:3307 -p 5508:3308 \
        ${IMAGE_NAME}
fi

singlestore-wait-start() {
  echo -n "Waiting for SingleStore to start..."
  while true; do
      if mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

singlestore-wait-start

if [[ "${EXISTS}" -eq 0 ]]; then
    echo
    echo "Creating aggregator node"
    docker exec ${CONTAINER_NAME} memsqlctl create-node --yes --password ${ROOT_PASSWORD} --port 3308
    docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key minimum_core_count --value 0
    docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key minimum_memory_mb --value 0
    docker exec ${CONTAINER_NAME} memsqlctl start-node --yes --all
    docker exec ${CONTAINER_NAME} memsqlctl add-aggregator --yes --host 127.0.0.1 --password ${ROOT_PASSWORD} --port 3308
fi

echo
echo "Setting up SSL"
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_ca --value /test-ssl/ca-cert.pem
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_cert --value /test-ssl/server-cert.pem
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_key --value /test-ssl/server-key.pem
echo "Setting up JWT"
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key jwt_auth_config_file --value /test-jwt/jwt_auth_config.json
echo "Restarting cluster"
docker restart ${CONTAINER_NAME}
singlestore-wait-start
echo "Setting up root-ssl user"
mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" -e 'create user "root-ssl"@"%" require ssl'
mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" -e 'grant all privileges on *.* to "root-ssl"@"%" require ssl with grant option'
mysql -u root -h 127.0.0.1 -P 5507 -p"${ROOT_PASSWORD}" -e 'create user "root-ssl"@"%" require ssl'
mysql -u root -h 127.0.0.1 -P 5507 -p"${ROOT_PASSWORD}" -e 'grant all privileges on *.* to "root-ssl"@"%" require ssl with grant option'
mysql -u root -h 127.0.0.1 -P 5508 -p"${ROOT_PASSWORD}" -e 'grant all privileges on *.* to "root-ssl"@"%" require ssl with grant option'
echo "Done!"

sleep 0.5
echo
echo "Ensuring child nodes are connected using container IP"
CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CONTAINER_NAME})
CURRENT_LEAF_IP=$(mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" --batch -N -e 'select host from information_schema.leaves')
if [[ ${CONTAINER_IP} != "${CURRENT_LEAF_IP}" ]]; then
    # remove leaf with current ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" --batch -N -e "remove leaf '${CURRENT_LEAF_IP}':3307"
    # add leaf with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" --batch -N -e "add leaf root:'${ROOT_PASSWORD}'@'${CONTAINER_IP}':3307"
fi
CURRENT_AGG_IP=$(mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" --batch -N -e 'select host from information_schema.aggregators where master_aggregator=0')
if [[ ${CONTAINER_IP} != "${CURRENT_AGG_IP}" ]]; then
    # remove aggregator with current ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" --batch -N -e "remove aggregator '${CURRENT_AGG_IP}':3308"
    # add aggregator with correct ip
    mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" --batch -N -e "add aggregator root:'${ROOT_PASSWORD}'@'${CONTAINER_IP}':3308"
fi

# create the database used in tests
mysql -u root -h 127.0.0.1 -P 5506 -p"${ROOT_PASSWORD}" -e 'create database if not exists test'

# setup PAM for tests
docker exec ${CONTAINER_NAME} bash -c 'printf "read password
[ \"\$PAM_USER\" == \"%s\" ] || exit 1
[ \"\$password\" == \"%s\" ] || exit 1
" "test_pam" \
  "test_pass" \
  > /tmp/s2_pamauth'

docker exec -iu 0 ${CONTAINER_NAME} bash -c 'printf "auth required pam_exec.so expose_authtok /bin/bash %s
account required pam_permit.so
session required pam_permit.so
password required pam_permit.so
" "/tmp/s2_pamauth" \
  > /etc/pam.d/s2_pam_test'

echo "Done!"
