package org.gbif.converters.parser.xml;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.concurrent.ForkJoinPool;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.core.io.SyncDataFileWriterBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ExtendedRecordConverterTest {

  private final int number = ForkJoinPool.getCommonPoolParallelism();

  private String inputPath;
  private String outPath;

  private final CodecFactory codec = CodecFactory.snappyCodec();

  private String getTestInputPath() {
    if (inputPath == null) {
      inputPath =
          getClass()
              .getResource("/responses/pages/7ef15372-1387-11e2-bb2e-00145eb45e9a/")
              .getFile();
    }
    return inputPath;
  }

  private String getTestOutPath() {
    if (outPath == null) {
      outPath = getTestInputPath() + "verbatim.avro";
    }
    return outPath;
  }

  @Test(expected = ParsingException.class)
  public void inputPathIsAbsentTest() throws Exception {
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro("", dataFileWrite);
    }
  }

  @Test(expected = ParsingException.class)
  public void outputPathIsAbsentTest() throws Exception {
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro("test", dataFileWrite);
    }
  }

  @Test(expected = ParsingException.class)
  public void inputPathIsNullTest() throws Exception {
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro(null, dataFileWrite);
    }
  }

  @Test(expected = NullPointerException.class)
  public void outputPathIsNullTest() throws Exception {
    try (SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(null)) {
      ExtendedRecordConverter.create(number).toAvro("test", dataFileWrite);
    }
  }

  @Test(expected = ParsingException.class)
  public void inputPathNotValidTest() throws Exception {
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro("test", dataFileWrite);
    }
  }

  @Test(expected = ParsingException.class)
  public void inputFileWrongExtensionTest() throws Exception {
    // State
    String inputPath = getTestInputPath() + "61.zip";

    // When
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro(inputPath, dataFileWrite);
    }
  }

  @Test
  public void parsingDirectoryTest() throws Exception {
    // State
    String inputPath = getTestInputPath() + "61";

    // When
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro(inputPath, dataFileWrite);
    }

    // Should
    File verbatim = new File(getTestOutPath());
    Assert.assertTrue(verbatim.exists());
    Files.deleteIfExists(verbatim.toPath());
  }

  @Test
  public void parsingArchiveTest() throws Exception {
    // State
    String inputPath = getTestInputPath() + "61.tar.xz";

    // When
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro(inputPath, dataFileWrite);
    }

    // Should
    File verbatim = new File(getTestOutPath());
    Assert.assertTrue(verbatim.exists());
    Files.deleteIfExists(verbatim.toPath());
  }

  @Test
  public void avroDeserializingTest() throws Exception {
    // State
    String inputPath = getTestInputPath() + "61";

    // When
    try (OutputStream output = new FileOutputStream(getTestOutPath());
        SyncDataFileWriter<ExtendedRecord> dataFileWrite = createWriter(output)) {
      ExtendedRecordConverter.create(number).toAvro(inputPath, dataFileWrite);
    }

    // Should
    File verbatim = new File(getTestOutPath());
    Assert.assertTrue(verbatim.exists());

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader =
        new DataFileReader<>(verbatim, datumReader)) {
      while (dataFileReader.hasNext()) {
        ExtendedRecord record = dataFileReader.next();
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getId());
      }
    }

    Files.deleteIfExists(verbatim.toPath());
  }

  private SyncDataFileWriter<ExtendedRecord> createWriter(OutputStream output) throws Exception {
    return SyncDataFileWriterBuilder.builder()
        .schema(ExtendedRecord.getClassSchema())
        .codec(codec.toString())
        .outputStream(output)
        .build()
        .createSyncDataFileWriter();
  }
}
