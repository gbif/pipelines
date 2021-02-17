package org.gbif.pipelines.ingest.java.transforms;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.junit.Assert;
import org.junit.Test;

public class OccurrenceHdfsRecordWriterTest {

  @Test
  public void writerSyncTest() throws IOException {

    // State
    Long gbifID = 777L;
    List<BasicRecord> list =
        Collections.singletonList(BasicRecord.newBuilder().setId("1").setGbifId(gbifID).build());
    Function<BasicRecord, OccurrenceHdfsRecord> fn =
        br -> {
          OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();
          hdfsRecord.setGbifid(br.getGbifId());
          return hdfsRecord;
        };

    String outputFile = getClass().getResource("/hdfsview/occurrence/").getFile();

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    // When
    OccurrenceHdfsRecordWriter.builder()
        .occurrenceHdfsRecordFn(fn)
        .basicRecords(list)
        .executor(Executors.newSingleThreadExecutor())
        .options(options)
        .build()
        .write();

    // Deserialize OccurrenceHdfsRecord from disk
    File result =
        new File(
            outputFile
                + "/d596fccb-2319-42eb-b13b-986c932780ad/146/interpreted/occurrence_hdfs_record/view_occurrence_d596fccb-2319-42eb-b13b-986c932780ad_146.avro");
    DatumReader<OccurrenceHdfsRecord> datumReader =
        new SpecificDatumReader<>(OccurrenceHdfsRecord.class);
    try (DataFileReader<OccurrenceHdfsRecord> dataFileReader =
        new DataFileReader<>(result, datumReader)) {
      while (dataFileReader.hasNext()) {
        OccurrenceHdfsRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertEquals(gbifID, record.getGbifid());
      }
    }

    Files.deleteIfExists(result.toPath());
  }

  @Test
  public void writerAsyncTest() throws IOException {

    // State
    Long gbifID = 777L;
    List<BasicRecord> list =
        Collections.singletonList(BasicRecord.newBuilder().setId("1").setGbifId(gbifID).build());
    Function<BasicRecord, OccurrenceHdfsRecord> fn =
        br -> {
          OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();
          hdfsRecord.setGbifid(br.getGbifId());
          return hdfsRecord;
        };

    String outputFile = getClass().getResource("/hdfsview/occurrence/").getFile();

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile,
      "--syncThreshold=0"
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    // When
    OccurrenceHdfsRecordWriter.builder()
        .occurrenceHdfsRecordFn(fn)
        .basicRecords(list)
        .executor(Executors.newSingleThreadExecutor())
        .options(options)
        .build()
        .write();

    // Deserialize OccurrenceHdfsRecord from disk
    File result =
        new File(
            outputFile
                + "/d596fccb-2319-42eb-b13b-986c932780ad/146/interpreted/occurrence_hdfs_record/view_occurrence_d596fccb-2319-42eb-b13b-986c932780ad_146.avro");
    DatumReader<OccurrenceHdfsRecord> datumReader =
        new SpecificDatumReader<>(OccurrenceHdfsRecord.class);
    try (DataFileReader<OccurrenceHdfsRecord> dataFileReader =
        new DataFileReader<>(result, datumReader)) {
      while (dataFileReader.hasNext()) {
        OccurrenceHdfsRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertEquals(gbifID, record.getGbifid());
      }
    }

    Files.deleteIfExists(result.toPath());
  }
}
