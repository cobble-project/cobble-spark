#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <path-to-cobble-java/java>" >&2
  exit 1
fi

COBBLE_JAVA_DIR="$(cd "$1" && pwd -P)"
MVN_CMD="${MVN_CMD:-mvn}"
PYTHON_CMD="${PYTHON_CMD:-}"

if [[ ! -f "${COBBLE_JAVA_DIR}/pom.xml" ]]; then
  echo "Missing pom.xml under ${COBBLE_JAVA_DIR}" >&2
  exit 1
fi

pushd "${COBBLE_JAVA_DIR}" >/dev/null

if [[ -z "${PYTHON_CMD}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
  else
    echo "Neither python3 nor python is available" >&2
    exit 1
  fi
fi

SOURCE_COORDS="$(
  "${PYTHON_CMD}" - "${COBBLE_JAVA_DIR}/pom.xml" <<'PY'
import sys
import xml.etree.ElementTree as ET

source_pom = sys.argv[1]
namespace = "http://maven.apache.org/POM/4.0.0"
root = ET.parse(source_pom).getroot()

def text_or_none(element_name):
    element = root.find(f"{{{namespace}}}{element_name}")
    return element.text if element is not None else None

artifact_id = text_or_none("artifactId")
version = text_or_none("version")

if version is None:
    parent = root.find(f"{{{namespace}}}parent")
    if parent is not None:
        version_element = parent.find(f"{{{namespace}}}version")
        version = version_element.text if version_element is not None else None

if artifact_id is None or version is None:
    raise SystemExit("Unable to resolve source artifact coordinates from pom.xml")

print(artifact_id)
print(version)
PY
)"

SOURCE_ARTIFACT_ID="$(printf '%s\n' "${SOURCE_COORDS}" | sed -n '1p')"
SOURCE_VERSION="$(printf '%s\n' "${SOURCE_COORDS}" | sed -n '2p')"

"${MVN_CMD}" --batch-mode --no-transfer-progress -DskipTests -Dspotless.check.skip=true clean package

JAR_PATH="target/${SOURCE_ARTIFACT_ID}-${SOURCE_VERSION}.jar"
if [[ ! -f "${JAR_PATH}" ]]; then
  echo "Expected jar not found: ${COBBLE_JAVA_DIR}/${JAR_PATH}" >&2
  exit 1
fi

"${MVN_CMD}" --batch-mode --no-transfer-progress \
  org.apache.maven.plugins:maven-install-plugin:3.1.2:install-file \
  -Dfile="${JAR_PATH}" \
  -DpomFile="${COBBLE_JAVA_DIR}/pom.xml"

popd >/dev/null
