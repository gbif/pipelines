#!/usr/bin/env bash

.buildSrc/mvnw spotless:apply clean verify -P gbif-artifacts,livingatlas-artifacts,extra-artifacts
