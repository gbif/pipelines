package org.gbif.pipelines.ingest.java.transforms;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.junit.Assert;
import org.junit.Test;

public class InterpretedAvroReaderTest {

  @Test
  public void readerTest() throws Exception {

    // State
    String outputFile = getClass().getResource("/").getFile() + "avro";

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(options, BasicTransform.builder().create(), "1")) {
      BasicRecord basicRecord = BasicRecord.newBuilder().setId("777").build();
      writer.append(basicRecord);
    }

    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(options, BasicTransform.builder().create(), "2")) {
      BasicRecord basicRecord = BasicRecord.newBuilder().setId("888").build();
      writer.append(basicRecord);
    }

    // When
    CompletableFuture<Map<String, BasicRecord>> result =
        InterpretedAvroReader.readAvroAsFuture(
            options, Executors.newSingleThreadExecutor(), BasicTransform.builder().create());
    Map<String, BasicRecord> map = result.get();

    // Should
    Assert.assertEquals(2, map.size());
  }
}
