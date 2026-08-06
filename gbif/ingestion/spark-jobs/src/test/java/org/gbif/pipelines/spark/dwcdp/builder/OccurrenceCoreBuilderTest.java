package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.spark.dwcdp.DwcDpRowTypes;
import org.gbif.pipelines.spark.dwcdp.builder.extension.AssertionExtensionBuilder;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OccurrenceCoreBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark = SparkTestSession.createBuilder().appName("OccurrenceCoreBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> occurrenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("eventID", DataTypes.StringType)
            .add("organismID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrencePkDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> organismDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("organismID", DataTypes.StringType)
            .add("organismName", DataTypes.StringType)
            .add("associatedOrganisms", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> occurrenceAssertionDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("assertionID", DataTypes.StringType)
            .add("occurrence_fk", DataTypes.StringType)
            .add("assertionType", DataTypes.StringType)
            .add("assertionValue", DataTypes.StringType)
            .add("assertionUnit", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- tests ----

  @Test
  void missingOccurrenceTable_throws() {
    assertThrows(
        IllegalStateException.class,
        () -> OccurrenceCoreBuilder.build(spark, TestTableLoader.of()),
        "Should throw when occurrence table is absent — routing error in orchestrator");
  }

  @Test
  void basicOccurrence_producesExtendedRecordWithCorrectCoreRowType() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", null, "Parus major")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ)).collectAsList();

    assertEquals(1, records.size());
    ExtendedRecord er = records.get(0);
    assertEquals("occ-1", er.getId());
    assertEquals(DwcDpRowTypes.CORE_ROW_TYPE_OCCURRENCE, er.getCoreRowType());
    assertTrue(er.getExtensions().isEmpty());
  }

  @Test
  void occurrenceWithNullId_isFiltered() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create(null, "evt-1", null, "Parus major"),
                RowFactory.create("occ-2", "evt-1", null, "Parus minor")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ)).collectAsList();

    assertEquals(1, records.size());
    assertEquals("occ-2", records.get(0).getId());
  }

  @Test
  void organismFieldsDenormalizedOntoCoreTerms() {
    Dataset<Row> occ =
        occurrenceDf(List.of(RowFactory.create("occ-1", "evt-1", "org-1", "Parus major")));
    Dataset<Row> orgDf =
        organismDf(List.of(RowFactory.create("org-1", "Blue tit", "sibling of:org-2")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(1, records.size());
    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals(
        "sibling of:org-2",
        coreTerms.get(DwcTerm.associatedOrganisms.qualifiedName()),
        "associatedOrganisms from organism table must appear on the occurrence core row");
  }

  @Test
  void multipleOccurrencesSameOrganism_eachGetsOrganismFields() {
    Dataset<Row> occ =
        occurrenceDf(
            List.of(
                RowFactory.create("occ-1", "evt-1", "org-1", "Parus major"),
                RowFactory.create("occ-2", "evt-2", "org-1", "Parus major")));
    Dataset<Row> orgDf = organismDf(List.of(RowFactory.create("org-1", "Blue tit", null)));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ, "organism", orgDf))
            .collectAsList();

    assertEquals(2, records.size());
    for (ExtendedRecord er : records) {
      assertEquals(
          "Blue tit",
          er.getCoreTerms().get(DwcTerm.organismName.qualifiedName()),
          "Each occurrence should carry the organism name — many:1 collapse");
    }
  }

  @Test
  void occurrenceAssertionTable_attachedAsEmofExtension() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Quercus robur")));
    Dataset<Row> assertionDf =
        occurrenceAssertionDf(List.of(RowFactory.create("A001", "OPK-001", "Mass", "3.2", "g")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(
                spark, TestTableLoader.of("occurrence", occ, "occurrence-assertion", assertionDf))
            .collectAsList();

    List<Map<String, String>> emof =
        records
            .get(0)
            .getExtensions()
            .get(AssertionExtensionBuilder.ROW_TYPE_EXTENDED_MEASUREMENT_OR_FACT);
    assertNotNull(emof, "eMoF extension must be present");
    assertEquals(1, emof.size());
    assertEquals("A001", emof.get(0).get(DwcTerm.measurementID.qualifiedName()));
    assertEquals("Mass", emof.get(0).get(DwcTerm.measurementType.qualifiedName()));
    assertEquals("3.2", emof.get(0).get(DwcTerm.measurementValue.qualifiedName()));
    assertEquals("g", emof.get(0).get(DwcTerm.measurementUnit.qualifiedName()));
  }

  @Test
  void occurrencePkSurrogate_neverLeaksIntoCoreTerms() {
    Dataset<Row> occ =
        occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "Quercus robur")));

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ)).collectAsList();

    assertEquals(1, records.size());
    assertFalse(
        records.get(0).getCoreTerms().containsKey("occurrence_pk"),
        "occurrence_pk is a surrogate key with no DwC term — it must never appear in coreTerms");
  }

  @Test
  void occurrencePkPresentButNoOccurrenceIdColumnAtAll_fallsBackToOccurrencePk() {
    // occurrenceID has no `required: true` constraint in the DwC-DP profile (only occurrence_pk
    // does), so a package that never populated it can legitimately arrive with the column absent
    // from the Parquet schema entirely — not merely null-valued. build() must not crash, and —
    // since occurrence_pk is required+unique — must not silently drop the record either: it falls
    // back to a synthesised "urn:gbif:dwcdp:occurrence:" + occurrence_pk id (see
    // CoreBuilderSupport.withIdFallback).
    StructType schema =
        new StructType()
            .add("occurrence_pk", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    Dataset<Row> occ =
        spark.createDataFrame(List.of(RowFactory.create("OPK-001", "Quercus robur")), schema);

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(spark, TestTableLoader.of("occurrence", occ)).collectAsList();

    assertEquals(1, records.size());
    assertEquals("urn:gbif:dwcdp:occurrence:OPK-001", records.get(0).getId());
    assertFalse(
        records.get(0).getCoreTerms().containsKey("occurrence_pk"),
        "occurrence_pk is a surrogate key with no DwC term — it must never appear in coreTerms, "
            + "even when it's also serving as the occurrenceID fallback");
  }

  @Test
  void materialFields_flowThroughToCoreTermsForGrscicollAndTripletId() {
    StructType occSchema =
        new StructType()
            .add("occurrenceID", DataTypes.StringType)
            .add("scientificName", DataTypes.StringType);
    Dataset<Row> occ =
        spark.createDataFrame(List.of(RowFactory.create("OCC001", "Parus major")), occSchema);

    StructType materialSchema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType)
            .add("institutionCode", DataTypes.StringType)
            .add("collectionCode", DataTypes.StringType)
            .add("catalogNumber", DataTypes.StringType);
    Dataset<Row> material =
        spark.createDataFrame(
            List.of(RowFactory.create("MEPK-1", "OCC001", "NHMD", "AVES", "12345")),
            materialSchema);

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(
                spark, TestTableLoader.of("occurrence", occ, "material", material))
            .collectAsList();

    assertEquals(1, records.size());
    Map<String, String> coreTerms = records.get(0).getCoreTerms();
    assertEquals("NHMD", coreTerms.get(DwcTerm.institutionCode.qualifiedName()));
    assertEquals("AVES", coreTerms.get(DwcTerm.collectionCode.qualifiedName()));
    assertEquals("12345", coreTerms.get(DwcTerm.catalogNumber.qualifiedName()));
    assertFalse(coreTerms.containsKey("evidenceForOccurrenceID"));
  }

  @Test
  void nucleotideAnalysis_reachesOccurrenceViaMaterialLink() {
    Dataset<Row> occ = occurrencePkDf(List.of(RowFactory.create("OPK-001", "OCC001", "sp")));

    StructType materialSchema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    Dataset<Row> material =
        spark.createDataFrame(List.of(RowFactory.create("MEPK-1", "OCC001")), materialSchema);

    StructType analysisSchema =
        new StructType()
            .add("nucleotideAnalysis_pk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("molecularProtocol_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType);
    Dataset<Row> analysis =
        spark.createDataFrame(
            List.of(RowFactory.create("NAPK-1", "MEPK-1", "MPPK-1", "NSPK-1")), analysisSchema);

    StructType sequenceSchema =
        new StructType()
            .add("nucleotideSequence_pk", DataTypes.StringType)
            .add("sequence", DataTypes.StringType);
    Dataset<Row> sequence =
        spark.createDataFrame(List.of(RowFactory.create("NSPK-1", "ACGT")), sequenceSchema);

    List<ExtendedRecord> records =
        OccurrenceCoreBuilder.build(
                spark,
                TestTableLoader.of(
                    "occurrence", occ,
                    "material", material,
                    "nucleotide-analysis", analysis,
                    "nucleotide-sequence", sequence))
            .collectAsList();

    assertEquals(1, records.size());
    List<Map<String, String>> dna =
        records
            .get(0)
            .getExtensions()
            .get(
                org.gbif.pipelines.spark.dwcdp.builder.extension.NucleotideExtensionBuilder
                    .ROW_TYPE_DNA_DERIVED_DATA);
    assertNotNull(dna, "DNA Derived Data extension must be present");
    assertEquals(1, dna.size());
    assertTrue(dna.get(0).containsValue("ACGT"));
  }
}
