#!/usr/bin/env bash
set -e

if [[ "$#" -ne 0 && "$#" -ne 1 ]]; then
    echo "Usage: $0 [USER_EMAIL]"
    exit 1
fi

if [[ "$#" -eq 0 ]]; then
    echo "{"
    echo "  \"version\": \"0.1.2\","
    echo "  \"username\": \"browserAuthUser\","
    echo "  \"token\": \"mock_token\","
    echo "  \"expiration\":\"2222-10-12T07:20:50.52Z\""
    echo "}"
fi

if [[ "$#" -eq 1 ]]; then
    if [[ "$1" -eq "fail" ]]; then
      exit 1
    fi
    echo "{"
    echo "  \"version\": \"0.1.2\","
    echo "  \"username\": \"$1\","
    echo "  \"token\": \"mock_token\","
    if [[ "$1" -eq "expire" ]]; then
      echo "  \"expiration\":\"2019-10-12T07:20:50.52Z\""
    else
      echo "  \"expiration\":\"2222-10-12T07:20:50.52Z\""
    fi
    echo "}"
fi

exit 0

