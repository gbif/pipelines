#!/bin/bash -e

IS_M2RELEASEBUILD=$1
POM_VERSION=$2

MODULE="pre-backbone-release"

IMAGE=docker.gbif.org/${MODULE}:${POM_VERSION}
IMAGE_LATEST=docker.gbif.org/${MODULE}:latest

echo "Building Docker image: ${IMAGE}"
docker build -f ./gbif/ingestion/${MODULE}/docker/Dockerfile ./gbif/ingestion/${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}-shaded.jar -t ${IMAGE}

echo "Pushing Docker image to the repository"
docker push ${IMAGE}
if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag pointing to the newly released ${IMAGE}"
  docker tag ${IMAGE} ${IMAGE_LATEST}
  docker push ${IMAGE_LATEST}
fi

echo "Removing local Docker image: ${IMAGE}"
docker rmi ${IMAGE} || true

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Removing local Docker image with latest tag: ${IMAGE_LATEST}"
  docker rmi ${IMAGE_LATEST}
fi
