#!/usr/bin/env bash
# Verify the workflow can authenticate to PyPI via OIDC Trusted Publishing,
# WITHOUT uploading anything.
#
# Performs the same handshake pypa/gh-action-pypi-publish runs before an
# upload, but stops the moment the token is minted:
#   1. Ask PyPI which OIDC audience to use.
#   2. Request a GitHub Actions OIDC token for that audience.
#   3. Exchange it at PyPI's mint-token endpoint for a short-lived,
#      project-scoped API token.
# A successful mint proves the Trusted Publisher matches this
# repo / workflow / environment. We mint and discard — nothing is published.
#
# Required env (present when the job declares `permissions: id-token: write`):
#   ACTIONS_ID_TOKEN_REQUEST_URL
#   ACTIONS_ID_TOKEN_REQUEST_TOKEN
# Optional:
#   PYPI_BASE_URL   — defaults to https://upload.pypi.org
#                     (the host that serves the OIDC endpoints; use
#                      https://test.pypi.org to verify against TestPyPI)

set -Eeuo pipefail

PYPI_BASE_URL="${PYPI_BASE_URL:-https://upload.pypi.org}"
PYPI_BASE_URL="${PYPI_BASE_URL%/}"
AUDIENCE_URL="${PYPI_BASE_URL}/_/oidc/audience"
MINT_URL="${PYPI_BASE_URL}/_/oidc/mint-token"

if [[ -z "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" || -z "${ACTIONS_ID_TOKEN_REQUEST_TOKEN:-}" ]]; then
  echo "ERROR: GitHub OIDC request vars are missing."
  echo "The job must declare 'permissions: id-token: write'."
  exit 1
fi

# 1. Audience — fall back to the well-known 'pypi' if the lookup is unavailable.
echo "Fetching OIDC audience from ${AUDIENCE_URL} ..."
AUDIENCE="$(curl --silent --show-error --fail "${AUDIENCE_URL}" 2>/dev/null \
  | jq -r '.audience // empty' || true)"
if [[ -z "$AUDIENCE" ]]; then
  echo "  audience lookup unavailable; falling back to 'pypi'"
  AUDIENCE="pypi"
fi
echo "  audience: ${AUDIENCE}"

# 2. GitHub Actions OIDC token for that audience.
echo "Requesting GitHub Actions OIDC token ..."
OIDC_JWT="$(curl --silent --show-error --fail \
  -H "Authorization: bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN}" \
  "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=${AUDIENCE}" \
  | jq -r '.value // empty')"
if [[ -z "$OIDC_JWT" ]]; then
  echo "ERROR: could not obtain a GitHub Actions OIDC token."
  exit 1
fi
# The OIDC JWT is a short-lived credential — keep it out of logs.
echo "::add-mask::${OIDC_JWT}"

# 3. Exchange for a PyPI API token (mint only — DO NOT upload).
echo "Exchanging OIDC token for a PyPI token at ${MINT_URL} ..."
HTTP_CODE="$(curl --silent --show-error \
  --output /tmp/pypi_mint_resp.json --write-out '%{http_code}' \
  -X POST --header 'Content-Type: application/json' \
  --data "$(jq -n --arg t "$OIDC_JWT" '{token: $t}')" \
  "${MINT_URL}")"

if [[ "$HTTP_CODE" != "200" ]]; then
  echo "ERROR: mint-token failed (HTTP ${HTTP_CODE})."
  echo "Response (token values are never present on failure):"
  cat /tmp/pypi_mint_resp.json || true
  rm -f /tmp/pypi_mint_resp.json
  exit 1
fi

# Success — discard the minted token without ever printing it.
rm -f /tmp/pypi_mint_resp.json
echo "OK: OIDC Trusted Publishing handshake succeeded (token minted and discarded)."
