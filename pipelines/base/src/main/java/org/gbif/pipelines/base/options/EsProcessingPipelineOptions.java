package org.gbif.pipelines.base.options;

import org.gbif.pipelines.base.options.base.EsOptions;

import org.apache.beam.sdk.annotations.Experimental;

@Experimental(Experimental.Kind.FILESYSTEM)
public interface EsProcessingPipelineOptions extends EsOptions, DataProcessingPipelineOptions {}
