package org.gbif.pipelines.ingest.java.transforms;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.converter.SyncDataFileWriterBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import lombok.SneakyThrows;

import static org.gbif.converters.converter.FsUtils.createParentDirectories;

public class ExtendedRecordReaderTest {

  private final Path verbatimPath = new Path("verbatim.avro");
  private final FileSystem verbatimFs = createParentDirectories(verbatimPath, null);

  @Test
  public void normalExtendedRecordsTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("2").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result = ExtendedRecordReader.readUniqueRecords(verbatimPath.toString());

    // Should
    assertMap(result, expectedOne, expectedTwo, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath.toString()));
  }

  @Test
  public void oneEqualDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result = ExtendedRecordReader.readUniqueRecords(verbatimPath.toString());

    // Should
    assertMap(result, expectedOne, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath.toString()));
  }

  @Test
  public void oneNotEqualDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1")
        .setCoreTerms(Collections.singletonMap("key", "value")).build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result = ExtendedRecordReader.readUniqueRecords(verbatimPath.toString());

    // Should
    assertMap(result, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath.toString()));
  }

  @Test
  public void allNotEqualDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1")
        .setCoreTerms(Collections.singletonMap("key1", "value")).build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("1")
        .setCoreTerms(Collections.singletonMap("key2", "value")).build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("1")
        .setCoreTerms(Collections.singletonMap("key3", "value")).build();
    writeExtendedRecords(expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result = ExtendedRecordReader.readUniqueRecords(verbatimPath.toString());

    // Should
    assertMap(result);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath.toString()));
  }

  private void assertMap(Map<String, ExtendedRecord> result, ExtendedRecord... expected) {
    Assert.assertEquals(expected.length, result.size());
    Arrays.stream(expected).forEach(exp -> {
      ExtendedRecord r = result.get(exp.getId());
      Assert.assertNotNull(r);
      Assert.assertEquals(exp, r);
    });
  }

  @SneakyThrows
  private void writeExtendedRecords(ExtendedRecord... records) {
    try (SyncDataFileWriter<ExtendedRecord> verbatimWriter = SyncDataFileWriterBuilder.builder()
        .schema(ExtendedRecord.getClassSchema())
        .codec("snappy")
        .outputStream(verbatimFs.create(verbatimPath))
        .syncInterval(2_097_152)
        .build()
        .createSyncDataFileWriter()) {
      Arrays.stream(records).forEach(verbatimWriter::append);
    }
  }

}
