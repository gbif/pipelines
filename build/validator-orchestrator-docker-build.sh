#!/bin/bash -e

IS_M2RELEASEBUILD=$1
POM_VERSION=$2
BASE_REGISTRY=${BASE_REGISTRY:-docker.gbif.org/third-party}

MODULE="validator-orchestrator"
MODULE_DIR="./gbif/validator/${MODULE}"
IMAGE="docker.gbif.org/${MODULE}:${POM_VERSION}"
IMAGE_LATEST="docker.gbif.org/${MODULE}:latest"

# The module produces both a library JAR and a runnable shaded JAR.
# Only the shaded JAR should be packaged in the Docker image.
JAR_DIR="${MODULE_DIR}/target"
JARS=("${JAR_DIR}"/*-shaded.jar)

if [[ ${#JARS[@]} -ne 1 || ! -f "${JARS[0]}" ]]; then
  echo >&2 "ERROR: Expected exactly one shaded JAR in ${JAR_DIR}."
  echo >&2 "Run 'mvn clean package' and check the module output."
  exit 1
fi

# Docker COPY paths are relative to the module build context.
JAR_FILE="target/$(basename "${JARS[0]}")"

echo "Building Docker image: ${IMAGE}"
echo "Using JAR: ${JAR_FILE}"
echo "Using base registry: ${BASE_REGISTRY}"

docker build \
  --build-arg BASE_REGISTRY="${BASE_REGISTRY}" \
  --build-arg JAR_FILE="${JAR_FILE}" \
  -f "${MODULE_DIR}/docker/Dockerfile" \
  -t "${IMAGE}" \
  "${MODULE_DIR}"

echo "Pushing Docker image: ${IMAGE}"
docker push "${IMAGE}"

if [[ "${IS_M2RELEASEBUILD}" == true ]]; then
  echo "Updating latest tag: ${IMAGE_LATEST}"
  docker tag "${IMAGE}" "${IMAGE_LATEST}"
  docker push "${IMAGE_LATEST}"
  docker rmi "${IMAGE_LATEST}" || true
fi

docker rmi "${IMAGE}" || true
