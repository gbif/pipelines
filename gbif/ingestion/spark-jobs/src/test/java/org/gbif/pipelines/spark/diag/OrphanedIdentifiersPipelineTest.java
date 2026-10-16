package org.gbif.pipelines.spark.diag;

import static org.gbif.pipelines.spark.Directories.IDENTIFIERS;
import static org.gbif.pipelines.spark.Directories.OCCURRENCE_JSON;
import static org.gbif.pipelines.spark.Directories.OCCURRENCE_VERBATIM;
import static org.gbif.pipelines.spark.Directories.SIMPLE_OCCURRENCE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.keygen.HBaseLockingKeyService;
import org.gbif.pipelines.keygen.hbase.HBaseStore;
import org.gbif.pipelines.spark.pojo.Occurrence;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrphanedIdentifiersPipelineTest {

  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  private static final String DATASET_A = "508089ca-ddb4-4112-b2cb-cb1bff8f39ad";
  private static final String DATASET_B = "168f92f9-8738-4959-8d42-e98cd18a7746";
  private static final String DATASET_NO_SUCCESS = "7ddf754f-d193-4cc9-b351-99906754a03b";
  private static final String DATASET_MISSING = "85339498-f762-11e1-a439-00145eb45e9a";

  private static SparkSession spark;
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws IOException {
    spark = SparkTestSession.createBuilder().appName("orphaned-identifiers-test").getOrCreate();
    fs = FileSystem.getLocal(new Configuration());
  }

  @AfterClass
  public static void tearDown() {
    if (spark != null) {
      spark.close();
    }
  }

  @Test
  public void orphanedKeysTest() throws Exception {

    String ingest = tmp.getRoot().getAbsolutePath() + "/ingest";

    // dataset A: attempt 2 is the latest successful one, occ3, occ4 and occ5 were dropped
    writeAttempt(ingest, DATASET_A, 1, true, true, records(rec("r1", "old1", null)), loaded("r1"));
    writeAttempt(
        ingest,
        DATASET_A,
        2,
        true,
        true,
        records(
            rec("r1", "occ1", "I|C|1|null").verbatim("I", "C", "1"),
            rec("r2", "occ2", null),
            // triplet from associatedKey
            rec("r3", "occ3", "I|C|3|null").verbatim("I", "C", "3"),
            // tripletValid=false run: triplet from the verbatim record, values trimmed
            rec("r4", "occ4", null).verbatim(" I ", "C\t", "4"),
            // no catalogNumber, so no triplet
            rec("r5", "occ5", null).verbatim("I", "C", " "),
            // triplet only record, can't be joined on uniqueKey
            rec("r6", null, "I|C|6|null").verbatim("I", "C", "6")),
        loaded("r1", "r2"));
    writeAttempt(ingest, DATASET_A, 3, false, true, records(rec("r1", "new1", null)), loaded());

    // dataset B: no verbatim directory, b2 only in simple_occurrence, occ3 matches only within B
    writeAttempt(
        ingest,
        DATASET_B,
        10,
        true,
        false,
        records(rec("b1", "b1", null), rec("o3", "occ3", null)),
        loaded("b1", "o3"),
        rec("b2", "b2", "B|C|2|null"));

    // no successful attempt
    writeAttempt(
        ingest, DATASET_NO_SUCCESS, 1, false, true, records(rec("x", "x", null)), loaded());

    String out = tmp.getRoot().getAbsolutePath() + "/out/orphaned_keys.csv";

    long count =
        OrphanedIdentifiersPipeline.run(
            spark,
            fs,
            Arrays.asList(DATASET_A, DATASET_B, DATASET_NO_SUCCESS, DATASET_MISSING),
            ingest,
            out);

    List<String> lines = Files.readAllLines(Path.of(out));
    assertEquals(
        Arrays.asList(
            "datasetKey,uniqueKey,hbaseKey,keyType",
            row(DATASET_B, "b2", "OCCURRENCE_ID"),
            row(DATASET_B, "B|C|2|null", "TRIPLET"),
            row(DATASET_A, "occ3", "OCCURRENCE_ID"),
            row(DATASET_A, "occ4", "OCCURRENCE_ID"),
            row(DATASET_A, "occ5", "OCCURRENCE_ID"),
            row(DATASET_A, "I|C|3|null", "TRIPLET"),
            row(DATASET_A, "I|C|4|null", "TRIPLET")),
        lines);
    assertEquals(7, count);
    assertFalse(
        Files.exists(Path.of(tmp.getRoot().getAbsolutePath(), "out", ".orphaned_keys.csv_tmp")));
  }

  @Test
  public void noOrphansTest() throws Exception {
    String ingest = tmp.getRoot().getAbsolutePath() + "/ingest";
    writeAttempt(
        ingest,
        DATASET_A,
        1,
        true,
        true,
        records(rec("r1", "occ1", "I|C|1|null").verbatim("I", "C", "1")),
        loaded("r1"));
    String out = tmp.getRoot().getAbsolutePath() + "/orphaned_keys.csv";

    long count = OrphanedIdentifiersPipeline.run(spark, fs, Arrays.asList(DATASET_A), ingest, out);

    assertEquals(0, count);
    assertTrue(Files.exists(Path.of(out)));
  }

  @Test
  public void lastSuccessfulAttemptTest() throws Exception {
    String ingest = tmp.getRoot().getAbsolutePath() + "/ingest";
    writeAttempt(ingest, DATASET_A, 9, true, true, records(rec("a", "a", null)), loaded("a"));
    writeAttempt(ingest, DATASET_A, 10, true, true, records(rec("a", "a", null)), loaded("a"));
    writeAttempt(ingest, DATASET_A, 11, false, true, records(rec("a", "a", null)), loaded("a"));

    Optional<org.apache.hadoop.fs.Path> attempt =
        OrphanedIdentifiersPipeline.findLastSuccessfulAttempt(
            fs, new org.apache.hadoop.fs.Path(ingest, DATASET_A));

    assertEquals("10", attempt.get().getName());
  }

  @Test
  public void hbaseKeyTest() {
    // a row from prod_h_occurrence_lookup
    assertEquals(
        "72:168f92f9-8738-4959-8d42-e98cd18a7746|7f906ef6-f1a1-11e6-884f-70e2840fc270",
        OrphanedIdentifiersPipeline.hbaseKey(
            "168f92f9-8738-4959-8d42-e98cd18a7746", "7f906ef6-f1a1-11e6-884f-70e2840fc270"));
    assertEquals(
        "54:168f92f9-8738-4959-8d42-e98cd18a7746|296f2b2d-8231-4357-9651-22301a4a3dba",
        OrphanedIdentifiersPipeline.hbaseKey(
            "168f92f9-8738-4959-8d42-e98cd18a7746", "296f2b2d-8231-4357-9651-22301a4a3dba"));
  }

  @Test
  public void tripletTest() {
    assertEquals("NBSB|Bird|1|null", OrphanedIdentifiersPipeline.triplet("NBSB", "Bird", "1"));
    assertEquals(
        "NBSB|Bird|1|null", OrphanedIdentifiersPipeline.triplet(" NBSB\t", "Bird\n", " 1 "));
    assertNull(OrphanedIdentifiersPipeline.triplet("NBSB", null, "1"));
    assertNull(OrphanedIdentifiersPipeline.triplet("NBSB", "Bird", "  "));
  }

  private static String row(String datasetKey, String uniqueKey, String keyType) {
    return String.join(",", datasetKey, uniqueKey, saltedKey(datasetKey, uniqueKey), keyType);
  }

  private static String saltedKey(String datasetKey, String uniqueKey) {
    return Bytes.toString(
        HBaseStore.saltKey(datasetKey + "|" + uniqueKey, HBaseLockingKeyService.NUMBER_OF_BUCKETS));
  }

  private static Rec rec(String id, String uniqueKey, String associatedKey) {
    return new Rec(id, uniqueKey, associatedKey);
  }

  private static List<Rec> records(Rec... recs) {
    return Arrays.asList(recs);
  }

  private static Set<String> loaded(String... ids) {
    return Arrays.stream(ids).collect(Collectors.toSet());
  }

  /**
   * Writes identifiers, simple_occurrence (the loaded records plus extraLoaded), optionally
   * verbatim and json/_SUCCESS for one attempt.
   */
  private static void writeAttempt(
      String ingest,
      String datasetKey,
      int attempt,
      boolean success,
      boolean withVerbatim,
      List<Rec> candidates,
      Set<String> loadedIds,
      Rec... extraLoaded)
      throws IOException {

    String dir = ingest + "/" + datasetKey + "/" + attempt;

    List<IdentifierRow> identifiers = new ArrayList<>();
    for (Rec r : candidates) {
      identifiers.add(new IdentifierRow(r.id, r.id + "-gbif", r.uniqueKey, r.associatedKey));
    }
    spark
        .createDataset(identifiers, Encoders.bean(IdentifierRow.class))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(dir + "/" + IDENTIFIERS);

    List<Rec> loaded =
        candidates.stream().filter(r -> loadedIds.contains(r.id)).collect(Collectors.toList());
    loaded.addAll(Arrays.asList(extraLoaded));
    List<Occurrence> occurrences = new ArrayList<>();
    for (Rec r : loaded) {
      // null keys are omitted from the serialised IdentifierRecord, as in real data
      String identifier =
          "{\"id\":\""
              + r.id
              + "\",\"internalId\":\""
              + r.id
              + "-gbif\","
              + (r.uniqueKey == null ? "" : "\"uniqueKey\":\"" + r.uniqueKey + "\",")
              + (r.associatedKey == null ? "" : "\"associatedKey\":\"" + r.associatedKey + "\",")
              + "\"issues\":{}}";
      occurrences.add(
          Occurrence.builder().id(r.id).internalId(r.id + "-gbif").identifier(identifier).build());
    }
    spark
        .createDataset(occurrences, Encoders.bean(Occurrence.class))
        .write()
        .mode(SaveMode.Overwrite)
        .parquet(dir + "/" + SIMPLE_OCCURRENCE);

    if (withVerbatim) {
      List<VerbatimRow> verbatim = new ArrayList<>();
      for (Rec r : candidates) {
        VerbatimRow v = new VerbatimRow();
        v.setId(r.id);
        v.setCoreTerms(r.coreTerms);
        verbatim.add(v);
      }
      spark
          .createDataset(verbatim, Encoders.bean(VerbatimRow.class))
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(dir + "/" + OCCURRENCE_VERBATIM);
    }

    Path json = Path.of(dir, OCCURRENCE_JSON);
    Files.createDirectories(json);
    if (success) {
      Files.createFile(json.resolve("_SUCCESS"));
    }
  }

  /** A test record: its identifier and verbatim triplet terms. */
  private static class Rec {
    final String id;
    final String uniqueKey;
    final String associatedKey;
    final Map<String, String> coreTerms = new HashMap<>();

    Rec(String id, String uniqueKey, String associatedKey) {
      this.id = id;
      this.uniqueKey = uniqueKey;
      this.associatedKey = associatedKey;
    }

    Rec verbatim(String institutionCode, String collectionCode, String catalogNumber) {
      coreTerms.put(DwcTerm.institutionCode.qualifiedName(), institutionCode);
      coreTerms.put(DwcTerm.collectionCode.qualifiedName(), collectionCode);
      coreTerms.put(DwcTerm.catalogNumber.qualifiedName(), catalogNumber);
      return this;
    }
  }

  /** Same columns as the IdentifierRecord parquet used by the pipelines. */
  @Data
  @NoArgsConstructor
  public static class IdentifierRow implements Serializable {
    private String id;
    private String internalId;
    private String uniqueKey;
    private String associatedKey;

    IdentifierRow(String id, String internalId, String uniqueKey, String associatedKey) {
      this.id = id;
      this.internalId = internalId;
      this.uniqueKey = uniqueKey;
      this.associatedKey = associatedKey;
    }
  }

  /** The ExtendedRecord columns of the verbatim parquet used here. */
  @Data
  @NoArgsConstructor
  public static class VerbatimRow implements Serializable {
    private String id;
    private Map<String, String> coreTerms;
  }
}
