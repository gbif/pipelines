#!/usr/bin/env bash

.buildSrc/mvnw spotless:apply clean install -DskipTests -DskipITs -T 1C -PgbifArtifacts -PlivingatlasArtifacts -PextraArtifacts
