#!/usr/bin/env bash

.buildSrc/mvnw spotless:apply clean package -DskipTests -DskipITs -T 1C -PextraArtifacts
