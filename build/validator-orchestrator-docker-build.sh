#!/bin/bash -e

IS_M2RELEASEBUILD=$1
POM_VERSION=$2

MODULE="validator-orchestrator"
MODULE_DIR="./gbif/validator/${MODULE}"
JAR_FILE="target/${MODULE}-${POM_VERSION}.jar"
IMAGE="docker.gbif.org/${MODULE}:${POM_VERSION}"
IMAGE_LATEST="docker.gbif.org/${MODULE}:latest"

if [[ ! -f "${MODULE_DIR}/${JAR_FILE}" ]]; then
  echo "Missing executable JAR: ${MODULE_DIR}/${JAR_FILE}" >&2
  echo "Ensure the module builds an executable Spring Boot JAR before Docker packaging." >&2
  exit 1
fi

echo "Building Docker image: ${IMAGE}"
docker build \
  -f "${MODULE_DIR}/docker/Dockerfile" \
  "${MODULE_DIR}" \
  --build-arg JAR_FILE="${JAR_FILE}" \
  -t "${IMAGE}"

echo "Pushing Docker image: ${IMAGE}"
docker push "${IMAGE}"

if [[ "${IS_M2RELEASEBUILD}" == true ]]; then
  echo "Updating latest tag: ${IMAGE_LATEST}"
  docker tag "${IMAGE}" "${IMAGE_LATEST}"
  docker push "${IMAGE_LATEST}"
  docker rmi "${IMAGE_LATEST}" || true
fi

docker rmi "${IMAGE}" || true
