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

public class MetadataPipeline {

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    run(options);
  }

  public static void run(DataProcessingPipelineOptions options) {
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
  }
}
