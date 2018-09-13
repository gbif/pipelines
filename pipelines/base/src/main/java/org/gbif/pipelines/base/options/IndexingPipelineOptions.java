package org.gbif.pipelines.base.options;

import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;

/**
 * Pipeline options (configuration) for GBIF based data indexing pipelines. Optionally can use a
 * {@link HadoopFileSystemOptions} when exporting/reading files.
 */
public interface IndexingPipelineOptions extends EsPipelineOptions, InterpretationPipelineOptions {}
