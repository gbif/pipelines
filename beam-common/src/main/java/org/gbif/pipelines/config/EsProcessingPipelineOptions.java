package org.gbif.pipelines.config;

import org.gbif.pipelines.config.base.EsOptions;

import org.apache.beam.sdk.annotations.Experimental;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends EsOptions, DataProcessingPipelineOptions {}
