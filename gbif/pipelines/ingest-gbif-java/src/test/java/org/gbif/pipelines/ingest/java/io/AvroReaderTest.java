package org.gbif.pipelines.ingest.java.io;

import static org.gbif.converters.converter.FsUtils.createParentDirectories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.converter.SyncDataFileWriterBuilder;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class AvroReaderTest {

  private final Path verbatimPath1 = new Path("target/verbatim1.avro");
  private final Path verbatimPath2 = new Path("target/verbatim2.avro");
  private final FileSystem verbatimFs = createParentDirectories(null, null, verbatimPath1);

  @Test
  public void regularExtendedRecordsTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("2").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readRecords("", "", ExtendedRecord.class, verbatimPath1.toString());

    // Should
    assertMap(result, expectedOne, expectedTwo, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
  }

  @Test
  public void regularExtendedRecordsWildcardTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("2").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    ExtendedRecord expectedFour = ExtendedRecord.newBuilder().setId("4").build();
    ExtendedRecord expectedFive = ExtendedRecord.newBuilder().setId("5").build();
    ExtendedRecord expectedSix = ExtendedRecord.newBuilder().setId("6").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);
    writeExtendedRecords(verbatimPath2, expectedFour, expectedFive, expectedSix);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readRecords(
            "", "", ExtendedRecord.class, new Path("target/verbatim*.avro").toString());

    // Should
    assertMap(
        result, expectedOne, expectedTwo, expectedThree, expectedFour, expectedFive, expectedSix);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
    Files.deleteIfExists(Paths.get(verbatimPath2.toString()));
  }

  @Test
  public void uniqueExtendedRecordsTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("2").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readUniqueRecords("", "", ExtendedRecord.class, verbatimPath1.toString());

    // Should
    assertMap(result, expectedOne, expectedTwo, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
  }

  @Test
  public void uniqueOneEqualDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readUniqueRecords("", "", ExtendedRecord.class, verbatimPath1.toString());

    // Should
    assertMap(result, expectedOne, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
  }

  @Test
  public void uniqueOneNotEqualDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("key", "value"))
            .build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readUniqueRecords("", "", ExtendedRecord.class, verbatimPath1.toString());

    // Should
    assertMap(result, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
  }

  @Test
  public void uniqueOneNotEqualDuplicateWildcardTest() throws IOException {

    // State
    ExtendedRecord expectedOne =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("key", "value"))
            .build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    ExtendedRecord expectedFour =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("key", "value"))
            .build();
    ExtendedRecord expectedFive = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedSix = ExtendedRecord.newBuilder().setId("3").build();

    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);
    writeExtendedRecords(verbatimPath2, expectedFour, expectedFive, expectedSix);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readUniqueRecords(
            "", "", ExtendedRecord.class, new Path("target/verbatim*.avro").toString());

    // Should
    assertMap(result, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
    Files.deleteIfExists(Paths.get(verbatimPath2.toString()));
  }

  @Test
  public void uniqueAllNotEqualDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("key1", "value"))
            .build();
    ExtendedRecord expectedTwo =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("key2", "value"))
            .build();
    ExtendedRecord expectedThree =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("key3", "value"))
            .build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readUniqueRecords("", "", ExtendedRecord.class, verbatimPath1.toString());

    // Should
    assertMap(result);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
  }

  private void assertMap(Map<String, ExtendedRecord> result, ExtendedRecord... expected) {
    Assert.assertEquals(expected.length, result.size());
    Arrays.stream(expected)
        .forEach(
            exp -> {
              ExtendedRecord r = result.get(exp.getId());
              Assert.assertNotNull(r);
              Assert.assertEquals(exp, r);
            });
  }

  @SneakyThrows
  private void writeExtendedRecords(Path path, ExtendedRecord... records) {
    try (SyncDataFileWriter<ExtendedRecord> verbatimWriter =
        SyncDataFileWriterBuilder.builder()
            .schema(ExtendedRecord.getClassSchema())
            .codec("snappy")
            .outputStream(verbatimFs.create(path))
            .syncInterval(2_097_152)
            .build()
            .createSyncDataFileWriter()) {
      Arrays.stream(records).forEach(verbatimWriter::append);
    }
  }
}
