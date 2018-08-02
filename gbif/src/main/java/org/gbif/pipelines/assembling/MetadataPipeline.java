package org.gbif.pipelines.assembling;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.core.ws.config.Config;
import org.gbif.pipelines.core.ws.config.HttpConfigFactory;
import org.gbif.pipelines.core.ws.config.Service;
import org.gbif.pipelines.io.avro.MetadataRecord;
import org.gbif.pipelines.transform.record.MetadataRecordTransform;
import org.gbif.pipelines.utils.FsUtils;

import java.nio.file.Paths;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MetadataPipeline creates metadata.avro @see with */
public class MetadataPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataPipeline.class);

  private DataProcessingPipelineOptions options;

  private MetadataPipeline(DataProcessingPipelineOptions options) {
    this.options = options;
  }

  public static MetadataPipeline from(DataProcessingPipelineOptions options) {
    return new MetadataPipeline(options);
  }

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    MetadataPipeline.from(options).run();
  }

  public void run() {

    LOG.info("Running metadata pipeline");

    String path =
        FsUtils.buildPathString(
            options.getDefaultTargetDirectory(),
            options.getDatasetId(),
            options.getAttempt().toString(),
            "metadata");

    Config wsConfig =
        HttpConfigFactory.createConfig(Service.DATASET_META, Paths.get(options.getWsProperties()));

    Pipeline pipeline = Pipeline.create(options);

    MetadataRecordTransform transform =
        MetadataRecordTransform.create(wsConfig).withAvroCoders(pipeline);

    pipeline
        .apply("Create collection from datasetId", Create.of(options.getDatasetId()))
        .apply("Metadata interpretation", transform)
        .get(transform.getDataTag())
        .apply("Metadata Kv2Value", Values.create())
        .apply(
            "Write to avro",
            AvroIO.write(MetadataRecord.class).to(path).withSuffix(".avro").withoutSharding());

    pipeline.run().waitUntilFinish();

    LOG.info("Finished metadata pipeline");
  }
}
