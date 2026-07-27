package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("unchecked")
class OccurrenceCoreTermsComplianceTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("OccurrenceCoreTermsComplianceTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  static final String CAUSE_OF_DEATH_RAW = "causeOfDeath";

  private static final Set<String> FORBIDDEN_CORE_TERMS_KEYS =
      Set.of("_corrupt_record", "mediaExtJson", "occurrenceExtJson", "__null_mask");

  // ---- fixtures ----

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType)
            .add("organismScope", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add(CAUSE_OF_DEATH_RAW, DataTypes.StringType)
            .add("organismRemarks", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> organismDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismScope", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add(CAUSE_OF_DEATH_RAW, DataTypes.StringType)
            .add("organismRemarks", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Row occurrenceRow(String occId) {
    return RowFactory.create(
        occId,
        "evt-1",
        "org-1",
        "Parus major",
        "multicellular organism",
        "Blue tit",
        "old age",
        "healthy individual");
  }

  private Row organismRow() {
    return RowFactory.create(
        "org-1",
        "DIFFERENT scope",
        "DIFFERENT name",
        "DIFFERENT cause",
        "DIFFERENT remarks",
        "sibling of:org-2");
  }

  // ---- baseline without organism join ----

  @Test
  void noOrganismJoin_coreTermsContainNoForbiddenKeys() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ)).collectAsList();

    assertEquals(1, records.size());
    assertNoForbiddenKeys(records.get(0).getCoreTerms(), "no-organism-join");
  }

  @Test
  void noOrganismJoin_exactExpectedTermsPresent() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ)).collectAsList();

    Set<String> expectedKeys =
        Set.of(
            DwcTerm.occurrenceID.qualifiedName(),
            DwcTerm.eventID.qualifiedName(),
            DwcTerm.organismID.qualifiedName(),
            DwcTerm.scientificName.qualifiedName(),
            DwcTerm.organismScope.qualifiedName(),
            DwcTerm.organismName.qualifiedName(),
            CAUSE_OF_DEATH_RAW,
            DwcTerm.organismRemarks.qualifiedName());

    assertExactKeys(expectedKeys, records.get(0).getCoreTerms(), "no-organism-join");
  }

  // ---- organism join: pollution guards ----

  @Test
  void afterOrganismJoin_coreTermsContainNoForbiddenKeys() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(1, records.size());
    assertNoForbiddenKeys(records.get(0).getCoreTerms(), "after-organism-join");
  }

  @Test
  void afterOrganismJoin_noDuplicateKeys() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    long distinctKeyCount = coreTerms.keySet().stream().distinct().count();
    assertEquals(coreTerms.size(), distinctKeyCount, "coreTerms must have no duplicate keys");
  }

  @Test
  void afterOrganismJoin_exactExpectedTermsPresent() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    Set<String> expectedKeys =
        Set.of(
            DwcTerm.occurrenceID.qualifiedName(),
            DwcTerm.eventID.qualifiedName(),
            DwcTerm.scientificName.qualifiedName(),
            DwcTerm.organismID.qualifiedName(),
            DwcTerm.organismScope.qualifiedName(),
            DwcTerm.organismName.qualifiedName(),
            CAUSE_OF_DEATH_RAW,
            DwcTerm.organismRemarks.qualifiedName(),
            DwcTerm.associatedOrganisms.qualifiedName());

    assertExactKeys(expectedKeys, records.get(0).getCoreTerms(), "after-organism-join");
  }

  // ---- per-field organism join correctness ----

  @Test
  void afterOrganismJoin_organismIdRetainedAsTermUri() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(
        "org-1",
        records.get(0).getCoreTerms().get(DwcTerm.organismID.qualifiedName()),
        "organismID must be retained in coreTerms — FK needed by downstream interpretation");
  }

  @Test
  void afterOrganismJoin_organismScopeOccurrenceValueWins() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(
        "multicellular organism",
        records.get(0).getCoreTerms().get(DwcTerm.organismScope.qualifiedName()),
        "occurrence.organismScope must win over organism.organismScope");
  }

  @Test
  void afterOrganismJoin_organismNameOccurrenceValueWins() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(
        "Blue tit",
        records.get(0).getCoreTerms().get(DwcTerm.organismName.qualifiedName()),
        "occurrence.organismName must win over organism.organismName");
  }

  @Test
  void afterOrganismJoin_causeOfDeathOccurrenceValueWins() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(
        "old age",
        records.get(0).getCoreTerms().get(CAUSE_OF_DEATH_RAW),
        "occurrence.causeOfDeath must win (raw key — not yet in dwc-api enum)");
  }

  @Test
  void afterOrganismJoin_organismRemarksOccurrenceValueWins() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(
        "healthy individual",
        records.get(0).getCoreTerms().get(DwcTerm.organismRemarks.qualifiedName()),
        "occurrence.organismRemarks must win over organism.organismRemarks");
  }

  @Test
  void afterOrganismJoin_associatedOrganismsContributedByOrganismTable() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(
        "sibling of:org-2",
        records.get(0).getCoreTerms().get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms must be contributed by the organism table");
  }

  @Test
  void afterOrganismJoin_nullOrganismId_organismsFieldsAbsent() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create("occ-1", "evt-1", null, "Parus major", null, null, null, null)));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertFalse(
        coreTerms.containsKey(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms must be absent when no organism match");
    assertFalse(
        coreTerms.containsKey(DwcTerm.organismID.qualifiedName()),
        "organismID must be absent when null");
  }

  // ---- helpers ----

  private static void assertNoForbiddenKeys(Map<String, String> coreTerms, String context) {
    for (String forbidden : FORBIDDEN_CORE_TERMS_KEYS) {
      assertFalse(
          coreTerms.containsKey(forbidden),
          "Forbidden key '" + forbidden + "' found in coreTerms [" + context + "]");
    }
    coreTerms
        .keySet()
        .forEach(
            key ->
                assertFalse(
                    key.startsWith("_"),
                    "Internal Spark column leaked into coreTerms [" + context + "]: " + key));
  }

  private static void assertExactKeys(
      Set<String> expectedKeys, Map<String, String> coreTerms, String context) {
    assertEquals(
        expectedKeys,
        coreTerms.keySet(),
        "coreTerms key mismatch ["
            + context
            + "] — missing: "
            + missingFrom(expectedKeys, coreTerms.keySet())
            + ", unexpected: "
            + missingFrom(coreTerms.keySet(), expectedKeys));
  }

  private static Set<String> missingFrom(Set<String> expected, Set<String> actual) {
    Set<String> missing = new HashSet<>(expected);
    missing.removeAll(actual);
    return missing;
  }
}
