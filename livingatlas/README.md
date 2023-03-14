# Living Atlas Pipelines extensions 
| | |
| ---- | ----|
| [![Build Status](https://api.travis-ci.org/gbif/pipelines.svg?branch=ala-dev)](http://travis-ci.org/gbif/pipelines) | Travis build for ala-dev branch |
| [![Build Status](https://builds.gbif.org/job/pipelines/badge/icon?subject=DEV%20-%20Build%20Status&style=flat-square)](https://builds.gbif.org/job/pipelines/)| GBIF Jenkins build for dev branch |
| [![Coverage](https://sonar.gbif.org/api/project_badges/measure?project=org.gbif.pipelines%3Apipelines-parent&metric=coverage)](https://sonar.gbif.org/dashboard?id=org.gbif.pipelines%3Apipelines-parent) |  Sonar  |

This module is to add functionality required by the Living Atlases to facilitate the replacement to [biocache-store](https://github.com/AtlasOfLivingAustralia/biocache-store) for data ingress. 

## Architecture

For details on the GBIF implementation, see the [pipelines github repository](https://github.com/gbif/pipelines).
This project is focussed on extensions to that architecture to support use by the Living Atlases.

![Pipelines](https://confluence.csiro.au/rest/gliffy/1.0/embeddedDiagrams/34f42b70-4936-4400-b0c4-01e4aeeb42c5.png"Pipelines") 

Above is a representation of the data flow from source data in Darwin core archives supplied by data providers, to the API access to these data via the biocache-service component.

Within the "Interpreted AVRO" box is a list of "transforms" each of which take the source data and produce an isolated output in a AVRO formatted file.

[GBIF's pipelines](https://github.com/gbif/pipelines) already supports a number of core transforms for handling biodiversity occurrence data. 
The Living Atlas pipelines extensions make us of these transforms "as-is" where possible and extend existing transforms where required. 

For information on how the architecture between the legacy system [biocache-store](https://github.com/AtlasOfLivingAustralia/biocache-store) 
and pipelines differ, [see this page](architectures.md).

## Dependent projects

The pipelines work has necessitated some minor additional API additions and change to the following components:

### biocache-service
[pipelines branch](https://github.com/AtlasOfLivingAustralia/biocache-service/tree/epic/pipelines%2Fdevelop) 
A version 3.x of biocache-service is in development.
This will not use Cassandra for storage of occurrence records, but Cassandra is still required for the storage
of user assertions and query identifiers (used to store large query parameters such as WKT strings).

### ala-namematching-service
A simple **drop wizard wrapper around the [ala-name-matching](https://github.com/AtlasOfLivingAustralia/ala-name-matching) library** 
has been developed to support integration with pipelines. 
This service is package in docker container as is deployed as a service using ansible.
For testing locally, use the [docker-compose files](pipelines/src/main/docker/ala-name-service.yml).

### ala-sensitive-data-service
A simple **drop wizard wrapper around the [ala-sensitive-data-service](https://github.com/AtlasOfLivingAustralia/ala-sensitive-data-service) library**
has been developed to support integration with pipelines.
This service is package in docker container as is deployed as a service using ansible.
For testing locally, use the [docker-compose files](pipelines/src/main/docker/ala-sensitive-data-service.yml).
 
## Getting started

Ansible scripts have been developed and are available [here](https://github.com/AtlasOfLivingAustralia/ala-install/tree/27_pipelines_spark_hadoop), 
Below are some instructions for setting up a local development environment for pipelines.
These steps will load a dataset into a SOLR index.

### Software requirements:

* Java 8 - this is mandatory (see [GBIF pipelines documentation](https://github.com/gbif/pipelines#about-the-project))
* Maven needs to run with OpenSDK 1.8 
'nano ~/.mavenrc' add 'export JAVA_HOME=[JDK1.8 PATH]'
* [Docker Desktop](https://www.docker.com/products/docker-desktop)
* [lombok plugin for intelliJ](https://projectlombok.org/setup/intellij) needs to be installed for slf4 annotation  
* Install `docopts` using the [prebuilt binary option](https://github.com/docopt/docopts#pre-built-binaries)
* Install `yq` via Brew (`brew install yq`)
* Optionally install the `avro-tools` package via Brew (`brew install avro-tools`)

### Setting up la-pipelines
  
1. Download shape files from [here](https://pipelines-shp.s3-ap-southeast-2.amazonaws.com/pipelines-shapefiles.zip) and expand into `/data/pipelines-shp` directory
1. Download a test darwin core archive (e.g. https://archives.ala.org.au/archives/gbif/dr893/dr893.zip)
1. Create the following directory `/data/pipelines-data`
1. Build with maven `mvn clean package`

### Running la-pipelines

1. Start required docker containers using
    ```bash
    docker-compose -f pipelines/src/main/docker/ala-name-service.yml up -d
    docker-compose -f pipelines/src/main/docker/solr8.yml up -d
    docker-compose -f pipelines/src/main/docker/ala-sensitive-data-service.yml 
    ```
    Note `ala-sensitive-data-service.yml` can be ommited if you don't need to run the SDS pipeline but you'll need to add
    ```yaml
    index:
      includeSensitiveData: false
    ```
    to the file `configs/la-pipelines-local.yaml`.
1. `cd scripts`
1. To convert DwCA to AVRO, run `./la-pipelines dwca-avro dr893`
1. To interpret, run `./la-pipelines interpret dr893 --embedded`
1. To mint UUIDs, run `./la-pipelines uuid dr893 --embedded`
1. (Optional) To sample run:
    1. `./la-pipelines sample dr893 --embedded`
1. To setup SOLR:
    1. Run `cd ../solr/scripts` and  then run ' `./update-solr-config.sh`
    1. Run `cd ../../scripts`
1. To create index avro files, run `./la-pipelines index dr893 --embedded`
2. To generate the SOLR index, run `./la-pipelines solr dr893 --embedded`
3. Check the SOLR index has records in the index by visiting http://localhost:8983/solr/#/biocache/query and clicking the "Execute query" button. It should show a non-zero number for `numFound` in the JSON response.
4. Run `./la-pipelines -h` for help and more steps:
```
LA-Pipelines data ingress utility.

The la-pipelines can be executed to run all the ingress steps or only a few of them:

Pipeline ingress steps:

    ┌───── do-all ───────────────────────────────────────────────┐
    │                                                            │
dwca-avro --> interpret --> validate --> uuid --> image-sync ... │
  --> image-load --> sds --> index --> sample --> jackknife --> solr
(...)
```

## Integration Tests

Tests follow the GBIF/failsafe/surefire convention. 
All integration tests have a suffix of "IT". 
All junit tests are ran with `mvn package` and integration tests are ran with `mvn verify`.

The integration tests will automatically start docker containers for the following:
* SOLR
* Elastic
* Name matching service
* SDS

## Code style and tools

For code style and tool see the [recommendations](https://github.com/gbif/pipelines#codestyle-and-tools-recommendations) on the GBIF pipelines project. In particular, note the project uses Project Lombok, please install Lombok plugin for Intellij IDEA.

`avro-tools` is recommended to aid to development for quick views of AVRO outputs. 
This can be installed on Macs with [Homebrew](https://brew.sh/) like so:

```
brew install avro-tools
```
