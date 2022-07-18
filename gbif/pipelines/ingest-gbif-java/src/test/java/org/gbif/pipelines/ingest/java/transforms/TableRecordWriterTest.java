package org.gbif.pipelines.ingest.java.transforms;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;
import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.gbif.pipelines.common.PipelinesVariables;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.Interpretation.InterpretationType;
import org.gbif.pipelines.common.beam.options.InterpretationPipelineOptions;
import org.gbif.pipelines.common.beam.options.PipelinesOptionsFactory;
import org.gbif.pipelines.common.beam.utils.PathBuilder;
import org.gbif.pipelines.io.avro.GbifIdRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.junit.Assert;
import org.junit.Test;

public class TableRecordWriterTest {

  @Test
  public void writerSyncTest() throws IOException {

    // State
    Long gbifID = 777L;

    GbifIdRecord idRecord = GbifIdRecord.newBuilder().setId("1").setGbifId(gbifID).build();
    GbifIdRecord skipIdRecord = GbifIdRecord.newBuilder().setId("1").setGbifId(-gbifID).build();
    List<GbifIdRecord> list = Arrays.asList(idRecord, skipIdRecord);

    Function<GbifIdRecord, Optional<OccurrenceHdfsRecord>> fn =
        id -> {
          if (id.getGbifId() < 0) {
            return Optional.empty();
          }
          OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();
          hdfsRecord.setGbifid(id.getGbifId().toString());
          return Optional.of(hdfsRecord);
        };

    String outputFile = getClass().getResource("/hdfsview/occurrence/").getFile();

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile,
      "--interpretationTypes=OCCURRENCE"
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    Function<InterpretationType, String> pathFn =
        st -> {
          String id = options.getDatasetId() + '_' + options.getAttempt() + AVRO_EXTENSION;
          return PathBuilder.buildFilePathViewUsingInputPath(
              options,
              PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE,
              st.name().toLowerCase(),
              id);
        };

    // When
    TableRecordWriter.<OccurrenceHdfsRecord>builder()
        .recordFunction(fn)
        .gbifIdRecords(list)
        .executor(Executors.newSingleThreadExecutor())
        .options(options)
        .targetPathFn(pathFn)
        .schema(OccurrenceHdfsRecord.getClassSchema())
        .recordType(OCCURRENCE)
        .types(options.getInterpretationTypes())
        .build()
        .write();

    // Deserialize OccurrenceHdfsRecord from disk
    File result =
        new File(
            outputFile
                + "/d596fccb-2319-42eb-b13b-986c932780ad/146/occurrence_table/occurrence/d596fccb-2319-42eb-b13b-986c932780ad_146.avro");
    DatumReader<OccurrenceHdfsRecord> datumReader =
        new SpecificDatumReader<>(OccurrenceHdfsRecord.class);
    try (DataFileReader<OccurrenceHdfsRecord> dataFileReader =
        new DataFileReader<>(result, datumReader)) {
      while (dataFileReader.hasNext()) {
        OccurrenceHdfsRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertEquals(gbifID.toString(), record.getGbifid());
      }
    }

    Files.deleteIfExists(result.toPath());
  }

  @Test
  public void writerAsyncTest() throws IOException {

    // State
    Long gbifID = 777L;

    GbifIdRecord idRecord = GbifIdRecord.newBuilder().setId("1").setGbifId(gbifID).build();
    GbifIdRecord skipIdRecord = GbifIdRecord.newBuilder().setId("1").setGbifId(-gbifID).build();
    List<GbifIdRecord> list = Arrays.asList(idRecord, skipIdRecord);

    Function<GbifIdRecord, Optional<OccurrenceHdfsRecord>> fn =
        id -> {
          if (id.getGbifId() < 0) {
            return Optional.empty();
          }
          OccurrenceHdfsRecord hdfsRecord = new OccurrenceHdfsRecord();
          hdfsRecord.setGbifid(id.getGbifId().toString());
          return Optional.of(hdfsRecord);
        };

    String outputFile = getClass().getResource("/hdfsview/occurrence/").getFile();

    String[] args = {
      "--datasetId=d596fccb-2319-42eb-b13b-986c932780ad",
      "--attempt=146",
      "--interpretationTypes=ALL",
      "--runner=SparkRunner",
      "--inputPath=" + outputFile,
      "--targetPath=" + outputFile,
      "--syncThreshold=0",
      "--interpretationTypes=OCCURRENCE"
    };
    InterpretationPipelineOptions options = PipelinesOptionsFactory.createInterpretation(args);

    Function<InterpretationType, String> pathFn =
        st -> {
          String id = options.getDatasetId() + '_' + options.getAttempt() + AVRO_EXTENSION;
          return PathBuilder.buildFilePathViewUsingInputPath(
              options,
              PipelinesVariables.Pipeline.Interpretation.RecordType.OCCURRENCE,
              st.name().toLowerCase(),
              id);
        };

    // When
    TableRecordWriter.<OccurrenceHdfsRecord>builder()
        .recordFunction(fn)
        .gbifIdRecords(list)
        .executor(Executors.newSingleThreadExecutor())
        .options(options)
        .targetPathFn(pathFn)
        .schema(OccurrenceHdfsRecord.getClassSchema())
        .recordType(OCCURRENCE)
        .types(options.getInterpretationTypes())
        .build()
        .write();

    // Deserialize OccurrenceHdfsRecord from disk
    File result =
        new File(
            outputFile
                + "/d596fccb-2319-42eb-b13b-986c932780ad/146/occurrence_table/occurrence/d596fccb-2319-42eb-b13b-986c932780ad_146.avro");
    DatumReader<OccurrenceHdfsRecord> datumReader =
        new SpecificDatumReader<>(OccurrenceHdfsRecord.class);
    try (DataFileReader<OccurrenceHdfsRecord> dataFileReader =
        new DataFileReader<>(result, datumReader)) {
      while (dataFileReader.hasNext()) {
        OccurrenceHdfsRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertEquals(gbifID.toString(), record.getGbifid());
      }
    }

    Files.deleteIfExists(result.toPath());
  }
}
