package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import org.gbif.pipelines.spark.util.TableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Verifies that the {@code coreTerms} map on occurrence-core {@link ExtendedRecord}s produced by
 * {@link OccurrenceCoreBuilder} satisfies schema compliance requirements after an organism join.
 *
 * <p>The DwC-DP organism table defines 6 organism-specific fields, mapped as follows:
 *
 * <table>
 *   <tr><th>Field</th><th>In DwcTerm enum?</th><th>coreTerms key</th></tr>
 *   <tr><td>organismID</td><td>Yes</td><td>{@link DwcTerm#organismID}.qualifiedName()</td></tr>
 *   <tr><td>organismScope</td><td>Yes</td><td>{@link DwcTerm#organismScope}.qualifiedName()</td></tr>
 *   <tr><td>organismName</td><td>Yes</td><td>{@link DwcTerm#organismName}.qualifiedName()</td></tr>
 *   <tr><td>associatedOrganisms</td><td>Yes</td><td>{@link DwcTerm#associatedOrganisms}.qualifiedName()</td></tr>
 *   <tr><td>organismRemarks</td><td>Yes</td><td>{@link DwcTerm#organismRemarks}.qualifiedName()</td></tr>
 *   <tr><td>causeOfDeath</td><td><b>No</b></td><td>raw string {@code "causeOfDeath"}</td></tr>
 * </table>
 *
 * <p>{@code causeOfDeath} is present in the DwC-DP organism schema (with a {@code
 * dcterms:isVersionOf} URI indicating it is a newer term) but is absent from the {@code DwcTerm}
 * enum in the version of {@code dwc-api} used by this project. {@link
 * org.gbif.pipelines.spark.dwcdp.builder.TermResolver} therefore cannot resolve it to a qualified
 * URI and falls back to the raw column name. This is intentional and correct behaviour — the raw
 * name is preserved rather than silently dropped. This test pins that behaviour explicitly so that
 * if a future upgrade of {@code dwc-api} adds {@code causeOfDeath} to the enum, the key in {@code
 * coreTerms} will change to the qualified URI and this test will fail, prompting a conscious
 * update.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceCoreTermsComplianceTest {

  /**
   * causeOfDeath is not in the DwcTerm enum — TermResolver falls back to the raw column name.
   * Pinned as a constant so usages are self-documenting and easy to update if dwc-api is upgraded.
   */
  static final String CAUSE_OF_DEATH_RAW = "causeOfDeath";

  private static final Set<String> FORBIDDEN_CORE_TERMS_KEYS =
      Set.of("_corrupt_record", "mediaExtJson", "occurrenceExtJson", "__null_mask");

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

  // ---- fixtures ----

  /**
   * Occurrence schema including all organism fields that are denormalized directly onto occurrence
   * per the DwC-DP occurrence schema specification.
   */
  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType)
            .add("organismScope", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("causeOfDeath", DataTypes.StringType) // not in DwcTerm enum — raw key
            .add("organismRemarks", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /**
   * Organism schema with all organism-table fields. The overlapping fields carry deliberately
   * different values so that precedence tests can verify which value appears in coreTerms.
   */
  private Dataset<Row> organismDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismScope", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("causeOfDeath", DataTypes.StringType) // not in DwcTerm enum — raw key
            .add("organismRemarks", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private TableLoader loaderWithOrganism(Dataset<Row> occurrenceDf, Dataset<Row> orgDf) {
    return tableName ->
        switch (tableName) {
          case "occurrence" -> Optional.of(occurrenceDf);
          case "organism" -> Optional.of(orgDf);
          default -> Optional.empty();
        };
  }

  private TableLoader loaderOccurrenceOnly(Dataset<Row> occurrenceDf) {
    return tableName ->
        "occurrence".equals(tableName) ? Optional.of(occurrenceDf) : Optional.empty();
  }

  /** Occurrence row with distinct values for each organism field. */
  private Row occurrenceRow(String occId) {
    return RowFactory.create(
        occId,
        "evt-1",
        "org-1",
        "Parus major",
        "multicellular organism", // organismScope — occurrence value
        "Great tit", // organismName — occurrence value
        "old age", // causeOfDeath — raw key, occurrence value
        "healthy individual"); // organismRemarks — occurrence value
  }

  /** Organism row with deliberately different values for all overlapping fields. */
  private Row organismRow() {
    return RowFactory.create(
        "org-1",
        "DIFFERENT scope", // must NOT appear in coreTerms
        "DIFFERENT name", // must NOT appear in coreTerms
        "DIFFERENT cause", // must NOT appear in coreTerms
        "DIFFERENT remarks", // must NOT appear in coreTerms
        "sibling of:org-2"); // only on organism — must be added
  }

  // ---- baseline without organism join ----

  @Test
  void noOrganismJoin_coreTermsContainNoForbiddenKeys() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderOccurrenceOnly(occ)).collectAsList();

    assertEquals(1, records.size());
    assertNoForbiddenKeys(records.get(0).getCoreTerms(), "no-organism-join");
  }

  @Test
  void noOrganismJoin_exactExpectedTermsPresent() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderOccurrenceOnly(occ)).collectAsList();

    // causeOfDeath appears as its raw column name — not a qualified URI — because it is absent
    // from the DwcTerm enum in the current version of dwc-api
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
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    assertEquals(1, records.size());
    assertNoForbiddenKeys(records.get(0).getCoreTerms(), "after-organism-join");
  }

  @Test
  void afterOrganismJoin_noDuplicateKeys() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    long distinctKeyCount = coreTerms.keySet().stream().distinct().count();
    assertEquals(coreTerms.size(), distinctKeyCount, "coreTerms must have no duplicate keys");
  }

  @Test
  void afterOrganismJoin_exactExpectedTermsPresent() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    Set<String> expectedKeys =
        Set.of(
            DwcTerm.occurrenceID.qualifiedName(),
            DwcTerm.eventID.qualifiedName(),
            DwcTerm.scientificName.qualifiedName(),
            DwcTerm.organismID.qualifiedName(),
            DwcTerm.organismScope.qualifiedName(),
            DwcTerm.organismName.qualifiedName(),
            CAUSE_OF_DEATH_RAW, // raw — not in DwcTerm enum
            DwcTerm.organismRemarks.qualifiedName(),
            DwcTerm.associatedOrganisms.qualifiedName()); // contributed by organism table

    assertExactKeys(expectedKeys, records.get(0).getCoreTerms(), "after-organism-join");
  }

  // ---- organism join: per-field correctness ----

  @Test
  void afterOrganismJoin_organismIdRetainedAsTermUri() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

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
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

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
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    assertEquals(
        "Great tit",
        records.get(0).getCoreTerms().get(DwcTerm.organismName.qualifiedName()),
        "occurrence.organismName must win over organism.organismName");
  }

  @Test
  void afterOrganismJoin_causeOfDeathOccurrenceValueWins() {
    // causeOfDeath is not in DwcTerm enum — key is the raw column name, not a qualified URI.
    // If dwc-api is upgraded and causeOfDeath is added to the enum, this test will fail because
    // the key will change to DwcTerm.causeOfDeath.qualifiedName() — update CAUSE_OF_DEATH_RAW
    // and this assertion accordingly.
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    assertEquals(
        "old age",
        records.get(0).getCoreTerms().get(CAUSE_OF_DEATH_RAW),
        "occurrence.causeOfDeath must win over organism.causeOfDeath (key is raw — not in DwcTerm enum)");
  }

  @Test
  void afterOrganismJoin_organismRemarksOccurrenceValueWins() {
    Dataset<Row> occ = occurrenceDf(List.of(occurrenceRow("occ-1")));
    Dataset<Row> orgDf = organismDf(List.of(organismRow()));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

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
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

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
        OccurrenceCoreBuilder.build(spark, loaderWithOrganism(occ, orgDf)).collectAsList();

    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertFalse(
        coreTerms.containsKey(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms must be absent when no organism match");
    assertFalse(
        coreTerms.containsKey(DwcTerm.organismID.qualifiedName()),
        "organismID must be absent when null — null omitted by RowTermMapper");
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
