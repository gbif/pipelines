package org.gbif.pipelines.ingest.options;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;

/**
 * Pipeline options (configuration) for GBIF based data indexing pipelines. Optionally can use a
 * {@link HadoopFileSystemOptions} when exporting/reading files.
 */
public interface EsIndexingPipelineOptions
    extends EsPipelineOptions, InterpretationPipelineOptions {}
