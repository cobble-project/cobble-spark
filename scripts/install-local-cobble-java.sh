#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <path-to-cobble-binding/cobble-java/java>" >&2
  exit 1
fi

COBBLE_JAVA_DIR="$(cd "$1" && pwd -P)"
MVN_CMD="${MVN_CMD:-mvn}"

if [[ ! -f "${COBBLE_JAVA_DIR}/pom.xml" ]]; then
  echo "Missing pom.xml under ${COBBLE_JAVA_DIR}" >&2
  exit 1
fi

# Let Maven install the source artifact and its POM under their declared coordinates.
cd "${COBBLE_JAVA_DIR}"
"${MVN_CMD}" --batch-mode --no-transfer-progress \
  -DskipTests -Dspotless.check.skip=true clean install
