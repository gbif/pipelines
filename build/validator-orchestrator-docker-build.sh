#!/bin/bash -e

IS_M2RELEASEBUILD=$1
POM_VERSION=$2
BASE_REGISTRY=${BASE_REGISTRY:-docker.gbif.org/third-party}

MODULE="validator-orchestrator"
MODULE_DIR="./gbif/validator/${MODULE}"
IMAGE="docker.gbif.org/${MODULE}:${POM_VERSION}"
IMAGE_LATEST="docker.gbif.org/${MODULE}:latest"

# Only the shaded JAR is runnable; the sibling library JAR is not a Docker artifact.
mapfile -d '' -t JARS < <(
  find "${MODULE_DIR}" -mindepth 3 -maxdepth 3 -type f \
    -path "*/target/*" \
    -name "*-${POM_VERSION}-shaded.jar" \
    -print0
)

if [[ ${#JARS[@]} -eq 0 ]]; then
  echo >&2 "ERROR: No shaded JAR found in ${MODULE_DIR}/*/target for version ${POM_VERSION}."
  echo >&2 "Run 'mvn package' before building the Docker image."
  exit 1
fi

if [[ ${#JARS[@]} -gt 1 ]]; then
  echo >&2 "ERROR: Found multiple shaded JARs:"
  printf >&2 '  %s\n' "${JARS[@]}"
  echo >&2 "Run 'mvn clean package' to produce an unambiguous build."
  exit 1
fi

# Docker COPY resolves paths relative to MODULE_DIR, our build context.
JAR_FILE="${JARS[0]#${MODULE_DIR}/}"

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
