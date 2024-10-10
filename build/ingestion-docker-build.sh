#Simple script for pushing a image containing the named modules build artifact
MODULE="ingest-gbif-beam"

POM_VERSION=$(mvn -q -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive exec:exec)
IMAGE=docker.gbif.org/${MODULE}:${POM_VERSION}

echo "Building Docker image: ${IMAGE}"
docker build -f ./gbif/ingestion/${MODULE}/docker/Dockerfile ./gbif/ingestion/${MODULE} --build-arg JAR_FILE=${MODULE}-${POM_VERSION}-shaded.jar -t ${IMAGE}

echo "Pushing Docker image to the repository"
docker push ${IMAGE}
if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Updated latest tag poiting to the newly released ${IMAGE}"
  docker tag ${IMAGE} docker.gbif.org/${MODULE}:latest
  docker push docker.gbif.org/${MODULE}:latest
fi

echo "Removing local Docker image: ${IMAGE}"
docker rmi ${IMAGE}

if [[ $IS_M2RELEASEBUILD = true ]]; then
  echo "Removing local Docker image with latest tag: docker.gbif.org/${MODULE}:latest"
  docker rmi docker.gbif.org/${MODULE}:latest
fi
