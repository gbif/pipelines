package org.gbif.pipelines.core.io;

public class AvroReaderTest {
  /*

  private final HdfsConfigs hdfsConfigs = HdfsConfigs.nullConfig();

  private final Path verbatimPath1 = new Path("target/verbatim1.avro");
  private final Path verbatimPath2 = new Path("target/verbatim2.avro");
  private final FileSystem verbatimFs = FsUtils.createParentDirectories(hdfsConfigs, verbatimPath1);

  @Test
  public void regularExtendedRecordsTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo = ExtendedRecord.newBuilder().setId("2").build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readRecords(hdfsConfigs, ExtendedRecord.class, verbatimPath1.toString());

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
            hdfsConfigs, ExtendedRecord.class, new Path("target/verbatim*.avro").toString());

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
        AvroReader.readUniqueRecords(hdfsConfigs, ExtendedRecord.class, verbatimPath1.toString());

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
        AvroReader.readUniqueRecords(hdfsConfigs, ExtendedRecord.class, verbatimPath1.toString());

    // Should
    assertMap(result, expectedOne, expectedThree);

    // Post
    Files.deleteIfExists(Paths.get(verbatimPath1.toString()));
  }

  @Test
  public void idDuplicateTest() throws IOException {

    // State
    ExtendedRecord expectedOne = ExtendedRecord.newBuilder().setId("1").build();
    ExtendedRecord expectedTwo =
        ExtendedRecord.newBuilder()
            .setId("1")
            .setCoreTerms(Collections.singletonMap("1", "2"))
            .build();
    ExtendedRecord expectedThree = ExtendedRecord.newBuilder().setId("3").build();
    writeExtendedRecords(verbatimPath1, expectedOne, expectedTwo, expectedThree);
    AtomicInteger counter = new AtomicInteger(0);

    // When
    Map<String, ExtendedRecord> result =
        AvroReader.readUniqueRecords(
            hdfsConfigs, ExtendedRecord.class, verbatimPath1.toString(), counter::incrementAndGet);

    // Should
    assertMap(result, expectedThree);
    Assert.assertEquals(1, counter.get());

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
        AvroReader.readUniqueRecords(hdfsConfigs, ExtendedRecord.class, verbatimPath1.toString());

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
            hdfsConfigs, ExtendedRecord.class, new Path("target/verbatim*.avro").toString());

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
        AvroReader.readUniqueRecords(hdfsConfigs, ExtendedRecord.class, verbatimPath1.toString());

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

   */
}
