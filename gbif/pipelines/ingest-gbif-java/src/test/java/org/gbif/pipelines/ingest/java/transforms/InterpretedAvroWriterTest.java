package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.DIRECTORY_NAME;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.transforms.core.BasicTransform;
import org.junit.Assert;
import org.junit.Test;

public class InterpretedAvroWriterTest {

  @Test
  public void writerTest() throws IOException {

    // State
    BasicTransform basicTransform = BasicTransform.builder().create();
    BasicRecord basicRecord = BasicRecord.newBuilder().setId("1").build();

    String id = "id";

    String outputFile = getClass().getResource("/").getFile() + "/" + DIRECTORY_NAME;

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--targetPath=" + outputFile
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    // When
    try (SyncDataFileWriter<BasicRecord> writer =
        InterpretedAvroWriter.createAvroWriter(options, basicTransform, id)) {
      writer.append(basicRecord);
    }

    // Deserialize BasicRecord from disk
    File result =
        new File(
            outputFile
                + "/d596fccb-2319-42eb-b13b-986c932780ad/146/interpreted/basic/interpret-id.avro");
    DatumReader<BasicRecord> datumReader = new SpecificDatumReader<>(BasicRecord.class);
    try (DataFileReader<BasicRecord> dataFileReader = new DataFileReader<>(result, datumReader)) {
      while (dataFileReader.hasNext()) {
        BasicRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertEquals(basicRecord.getId(), record.getId());
      }
    }

    Files.deleteIfExists(result.toPath());
  }
}
