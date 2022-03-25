#!/usr/bin/env bash
set -e

if [[ "$#" -ne 0 && "$#" -ne 1 ]]; then
    echo "Usage: $0 [USER_EMAIL]"
    exit 1
fi

if [[ "$#" -eq 0 ]]; then
    echo "{"
    echo "  \"version\": \"0.1.2\","
    echo "  \"username\": \"SPCM1DAADM3DF001\","
    echo "  \"email\": \"test@singlestore.com\","
    echo "  \"token\": \"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBfZGlzcGxheW5hbWUiOiJTUENNMURBQURNM0RGMDAxIiwibmFtZSI6IkpvaG4gRG9lIiwiZXhwIjoxNzE2MjM5MDIwfQ.DWtsZ4Hqg-AWCPaEjV-Q6vfTJSbD4m7LWcngcG0lBZV53mOzBvKEcsrM2hRUOtUaNP_RbFDG9KOhsyBEz7351ZNRQhiApjJiYdNlfG934YxHtmxyJQjQzhvW7AF3iSnLMttArWMUhZ7Y7KA2vcGkrRrYOy9UsbcJr0JPU63ehvIshqQADKaZ08ws7jeUczYX9hB20Mg_WAVOCRVkTgT-arrS0Do7DKbzhuaz9ajcks5Zbr7zJSR8GIDYfrfMwTXVm_IARhBXCDjvkr21qqlsbSEOwPE0eK9C_k8SKmP8zTdMAlMiuQx1Dfd9IUUjemfcfxRtHJkbR3utZUNyLf3-3Q\","
    echo "  \"expiration\":\"2222-10-12T07:20:50.52Z\""
    echo "}"
fi

if [[ "$#" -eq 1 ]]; then
    if [[ "$1" == "fail" ]]; then
      exit 1
    fi
    echo "{"
    echo "  \"version\": \"0.1.2\","
    echo "  \"username\": \"$1\","
    echo "  \"email\": \"test@singlestore.com\","
    echo "  \"token\": \"mock_token\","
    if [[ "$1" == "expire" ]]; then
      echo "  \"expiration\":\"2019-10-12T07:20:50.52Z\""
    else
      echo "  \"expiration\":\"2222-10-12T07:20:50.52Z\""
    fi
    echo "}"
fi

exit 0

