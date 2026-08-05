package org.gbif.pipelines.spark.dwcdp.builder.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.gbif.pipelines.spark.util.MapperUtil;
import org.gbif.pipelines.spark.util.SparkTestSession;
import org.gbif.pipelines.spark.util.TestTableLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NucleotideExtensionBuilderTest {

  SparkSession spark;

  @BeforeAll
  void setup() {
    spark =
        SparkTestSession.createBuilder().appName("NucleotideExtensionBuilderTest").getOrCreate();
  }

  @AfterAll
  void teardown() {
    spark.stop();
  }

  // ---- fixtures ----

  private Dataset<Row> eventPkDf(List<Row> rows) {
    StructType schema =
        new StructType().add("event_pk", DataTypes.StringType).add("eventID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> sequenceDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("nucleotideSequence_pk", DataTypes.StringType)
            .add("sequence", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> protocolDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("molecularProtocol_pk", DataTypes.StringType)
            .add("target_gene", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /** {@code nucleotide-analysis} with both optional FKs — event-only rows leave materialFk null. */
  private Dataset<Row> analysisDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("nucleotideAnalysis_pk", DataTypes.StringType)
            .add("event_fk", DataTypes.StringType)
            .add("materialEntity_fk", DataTypes.StringType)
            .add("molecularProtocol_fk", DataTypes.StringType)
            .add("nucleotideSequence_fk", DataTypes.StringType)
            .add("readCount", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  private Dataset<Row> materialDf(List<Row> rows) {
    StructType schema =
        new StructType()
            .add("materialEntity_pk", DataTypes.StringType)
            .add("evidenceForOccurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  /**
   * {@code MaterialJoinBuilder.singleMaterialOccurrenceLinks} only resolves {@code
   * evidenceForOccurrenceID} against occurrences that exist <em>locally</em> in this package — an
   * {@code occurrence} table with a matching {@code occurrenceID} must be present, or every
   * material row is treated as referencing something outside the package and nothing resolves.
   */
  private Dataset<Row> occurrenceIdOnlyDf(List<Row> rows) {
    StructType schema = new StructType().add("occurrenceID", DataTypes.StringType);
    return spark.createDataFrame(rows, schema);
  }

  // ---- absent-table cases ----

  @Test
  void nucleotideAnalysisAbsent_bothPathsReturnEmpty() {
    assertTrue(
        NucleotideExtensionBuilder.buildEvent(
                spark, TestTableLoader.of("event", eventPkDf(List.of())))
            .isEmpty());
    assertTrue(NucleotideExtensionBuilder.buildOccurrence(spark, TestTableLoader.of()).isEmpty());
  }

  // ---- event path (eDNA/metabarcoding: event_fk populated, no material) ----

  @Test
  void eventLinkedAnalysis_resolvesToEventWithSequenceAndProtocol() throws Exception {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> analysis =
        analysisDf(
            List.of(RowFactory.create("NAPK-1", "EPK-001", null, "MPPK-1", "NSPK-1", "1200")));
    Dataset<Row> sequence = sequenceDf(List.of(RowFactory.create("NSPK-1", "ACGTACGT")));
    Dataset<Row> protocol = protocolDf(List.of(RowFactory.create("MPPK-1", "16S")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildEvent(
            spark,
            TestTableLoader.of(
                "event",
                event,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_ANALYSIS,
                analysis,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_SEQUENCE,
                sequence,
                NucleotideExtensionBuilder.TABLE_MOLECULAR_PROTOCOL,
                protocol));

    assertTrue(result.isPresent());
    Row row = result.get().first();
    assertEquals("EVT001", row.getAs("eventID"));
    List<Map<String, String>> ext = parseDnaJson(row);
    assertEquals(1, ext.size());
    // Term keys are asserted by value, not by exact map key: whether TermFactory resolves
    // "sequence"/"target_gene"/"readCount" to a qualified MIxS/GbifDna URI or leaves them as
    // raw column names (see class docs — no confirmed rename map for these yet) is a dwc-api
    // version detail this test should not be coupled to; either way the values must survive.
    assertTrue(ext.get(0).containsValue("ACGTACGT"));
    assertTrue(ext.get(0).containsValue("16S"));
    assertTrue(ext.get(0).containsValue("1200"));
  }

  @Test
  void analysisWithBothFksPopulated_excludedFromEventPath() {
    // materialEntity_fk also populated -> attaches via buildOccurrence instead; buildEvent must
    // not also emit it under the event, or the same analysis would appear twice.
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> analysis =
        analysisDf(
            List.of(RowFactory.create("NAPK-1", "EPK-001", "MEPK-1", "MPPK-1", "NSPK-1", "1200")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildEvent(
            spark, TestTableLoader.of("event", event, "nucleotide-analysis", analysis));

    assertTrue(
        result.isEmpty(),
        "a row with materialEntity_fk populated belongs to the occurrence path only");
  }

  @Test
  void danglingEventFk_analysisRowDropped() {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> analysis =
        analysisDf(
            List.of(RowFactory.create("NAPK-1", "EPK-DANGLING", null, "MPPK-1", "NSPK-1", "1200")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildEvent(
            spark, TestTableLoader.of("event", event, "nucleotide-analysis", analysis));

    assertTrue(result.isEmpty());
  }

  // ---- occurrence path (physical-specimen: materialEntity_fk populated) ----

  @Test
  void materialLinkedAnalysis_resolvesToOccurrenceViaSingleMaterialLink() throws Exception {
    Dataset<Row> occurrence = occurrenceIdOnlyDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material = materialDf(List.of(RowFactory.create("MEPK-1", "OCC001")));
    Dataset<Row> analysis =
        analysisDf(List.of(RowFactory.create("NAPK-1", null, "MEPK-1", "MPPK-1", "NSPK-1", "800")));
    Dataset<Row> sequence = sequenceDf(List.of(RowFactory.create("NSPK-1", "TTGGCCAA")));
    Dataset<Row> protocol = protocolDf(List.of(RowFactory.create("MPPK-1", "ITS2")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildOccurrence(
            spark,
            TestTableLoader.of(
                "occurrence",
                occurrence,
                "material",
                material,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_ANALYSIS,
                analysis,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_SEQUENCE,
                sequence,
                NucleotideExtensionBuilder.TABLE_MOLECULAR_PROTOCOL,
                protocol));

    assertTrue(result.isPresent());
    Row row = result.get().first();
    assertEquals("OCC001", row.getAs("occurrenceID"));
    List<Map<String, String>> ext = parseDnaJson(row);
    assertTrue(ext.get(0).containsValue("TTGGCCAA"));
    assertTrue(ext.get(0).containsValue("ITS2"));
  }

  @Test
  void materialLinkedAnalysis_notResolvedWhenMaterialAmbiguous() {
    // Two material rows citing the same occurrence -> MaterialJoinBuilder.
    // singleMaterialOccurrenceLinks deliberately resolves neither, so the analysis has nowhere
    // unambiguous to attach.
    Dataset<Row> occurrence = occurrenceIdOnlyDf(List.of(RowFactory.create("OCC001")));
    Dataset<Row> material =
        materialDf(
            List.of(RowFactory.create("MEPK-1", "OCC001"), RowFactory.create("MEPK-2", "OCC001")));
    Dataset<Row> analysis =
        analysisDf(List.of(RowFactory.create("NAPK-1", null, "MEPK-1", "MPPK-1", "NSPK-1", "800")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildOccurrence(
            spark,
            TestTableLoader.of(
                "occurrence", occurrence, "material", material, "nucleotide-analysis", analysis));

    assertTrue(result.isEmpty());
  }

  // ---- resilience: missing sequence/protocol tables never drop the analysis row ----

  @Test
  void moleculaProtocolTableAbsent_analysisRowSurvivesWithoutIt() throws Exception {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> analysis =
        analysisDf(
            List.of(RowFactory.create("NAPK-1", "EPK-001", null, "MPPK-1", "NSPK-1", "1200")));
    Dataset<Row> sequence = sequenceDf(List.of(RowFactory.create("NSPK-1", "ACGTACGT")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildEvent(
            spark,
            TestTableLoader.of(
                "event",
                event,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_ANALYSIS,
                analysis,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_SEQUENCE,
                sequence));

    assertTrue(
        result.isPresent(),
        "a missing molecular-protocol table must not drop the whole analysis row");
    List<Map<String, String>> ext = parseDnaJson(result.get().first());
    assertTrue(ext.get(0).containsValue("ACGTACGT"));
    assertFalse(
        ext.get(0).containsKey("molecularProtocol_fk"),
        "the unresolved surrogate FK must not leak through as a meaningless raw value");
  }

  // ---- fanout safety: two analyses sharing one molecular-protocol must not cross-multiply ----

  @Test
  void multipleAnalysesSharingOneProtocol_noCartesianFanout() throws Exception {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> analysis =
        analysisDf(
            List.of(
                RowFactory.create("NAPK-1", "EPK-001", null, "MPPK-1", "NSPK-1", "100"),
                RowFactory.create("NAPK-2", "EPK-001", null, "MPPK-1", "NSPK-2", "200")));
    Dataset<Row> sequence =
        sequenceDf(
            List.of(RowFactory.create("NSPK-1", "AAAA"), RowFactory.create("NSPK-2", "CCCC")));
    Dataset<Row> protocol = protocolDf(List.of(RowFactory.create("MPPK-1", "16S")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildEvent(
            spark,
            TestTableLoader.of(
                "event",
                event,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_ANALYSIS,
                analysis,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_SEQUENCE,
                sequence,
                NucleotideExtensionBuilder.TABLE_MOLECULAR_PROTOCOL,
                protocol));

    assertTrue(result.isPresent());
    List<Map<String, String>> ext = parseDnaJson(result.get().first());
    assertEquals(2, ext.size(), "one row per analysis — sharing a protocol must not multiply rows");
  }

  // ---- surrogate keys never leak ----

  @Test
  void surrogateKeysNeverLeakIntoDnaExtension() throws Exception {
    Dataset<Row> event = eventPkDf(List.of(RowFactory.create("EPK-001", "EVT001")));
    Dataset<Row> analysis =
        analysisDf(
            List.of(RowFactory.create("NAPK-1", "EPK-001", null, "MPPK-1", "NSPK-1", "1200")));
    Dataset<Row> sequence = sequenceDf(List.of(RowFactory.create("NSPK-1", "ACGTACGT")));
    Dataset<Row> protocol = protocolDf(List.of(RowFactory.create("MPPK-1", "16S")));

    Optional<Dataset<Row>> result =
        NucleotideExtensionBuilder.buildEvent(
            spark,
            TestTableLoader.of(
                "event",
                event,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_ANALYSIS,
                analysis,
                NucleotideExtensionBuilder.TABLE_NUCLEOTIDE_SEQUENCE,
                sequence,
                NucleotideExtensionBuilder.TABLE_MOLECULAR_PROTOCOL,
                protocol));

    Map<String, String> item = parseDnaJson(result.get().first()).get(0);
    for (String surrogate :
        Arrays.asList(
            "nucleotideAnalysis_pk",
            "nucleotideSequence_pk",
            "molecularProtocol_pk",
            "nucleotideSequence_fk",
            "molecularProtocol_fk",
            "event_fk",
            "materialEntity_fk")) {
      assertFalse(item.containsKey(surrogate), surrogate + " must not leak into the DNA extension");
    }
  }

  // ---- helper ----

  @SuppressWarnings("unchecked")
  private List<Map<String, String>> parseDnaJson(Row row) throws Exception {
    String json = row.getAs(NucleotideExtensionBuilder.COL_DNA_EXT_JSON);
    return MapperUtil.MAPPER.readValue(json, new TypeReference<List<Map<String, String>>>() {});
  }
}
