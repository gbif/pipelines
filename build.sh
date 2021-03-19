#!/usr/bin/env bash

.buildSrc/mvnw clean verify package install -PextraArtifacts
